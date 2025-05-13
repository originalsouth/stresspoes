#!/usr/bin/env python

import inspect
import json
import random
import time

import click
import zstandard as zstd
from dill import dumps, loads
from octopoes_client import OctopoesClient
from term_image.image import from_file
from xxhash import xxh3_128_hexdigest as xxh3

MAGIC = 0xC0DECA7


@click.group(
    context_settings={
        "help_option_names": ["-h", "--help"],
        "max_content_width": 120,
        "show_default": True,
    }
)
@click.option("-u", "--url", default="http://localhost:8001", help="Octopoes base url")
@click.option("-o", "--org", default="0", help="Orginization")
@click.option("-s", "--silent", is_flag=True, help="Silent")
@click.pass_context
def cli(ctx: click.Context, url: str, org: str, silent: bool):
    if not silent:
        image = from_file("stresspoes.jpg")
        image.draw()
        click.echo("Hello from stresspoes!")
    oc = OctopoesClient(url, org)
    ctx.ensure_object(dict)
    ctx.obj["organisation"] = org
    ctx.obj["client"] = oc


@cli.command(help="Make an Octopoes session datamap image")
@click.argument("filename", default="datamap.kat")
@click.pass_context
def datamap(ctx: click.Context, filename: str):
    oc = ctx.obj["client"]
    oois = {obj["primary_key"]: obj for obj in oc.objects()["items"]}
    affirmations = oc.origins(origin_type="affirmation")
    declarations = oc.origins(origin_type="declaration")
    observations = oc.origins(origin_type="observation")
    datamap = {
        "organisation": ctx.obj["organisation"],
        "oois": oois,
        "affirmations": affirmations,
        "declarations": declarations,
        "observations": observations,
    }
    with open(filename, "wb") as file:
        payload = dumps(datamap)
        file.write(
            zstd.ZstdCompressor().compress(dumps((MAGIC, payload, xxh3(payload))))
        )


@cli.command(
    help="Stress Octopoes (warning: this may destroy your OpenKAT installation)"
)
@click.option("-d", "--dump", is_flag=True, default=False, help="Dump opbjects diff")
@click.option(
    "-x",
    "--xterminate",
    is_flag=True,
    default=True,
    help="Delete resulting organisation",
)
@click.argument("filename", default="datamap.kat")
@click.pass_context
def stress(ctx: click.Context, filename: str, dump: bool, xterminate: bool):
    oc = ctx.obj["client"]
    with open(filename, "rb") as file:
        magic, datamap, checksum = loads(
            zstd.ZstdDecompressor().decompress(file.read())
        )
    if magic != MAGIC or checksum != xxh3(datamap):
        click.echo(f"Datamap file {filename} seems corrupted.")
        return
    else:
        datamap = loads(datamap)
    if datamap["organisation"] == oc.org:
        organisation = (
            oc.org
            + f"-{"".join(chr(random.choice(range(97, 122))) for _ in range(16))}"
        )
        noc = OctopoesClient(oc.url, organisation)
        print(f"organisation: {organisation}")
        noc.node_create(organisation)
        time.sleep(0.5)
        noc = OctopoesClient(oc.url, organisation)
    else:
        organisation = oc.org
        noc = oc
    res = noc.save_many_declarations(
        [{"ooi": datamap["oois"][decl["source"]]} for decl in datamap["declarations"]]
    )
    if res is not None:
        print(f"FAIL({inspect.currentframe().f_lineno}): {json.dumps(res, indent=2)}")
    res = noc.save_many_scan_profile(
        [
            datamap["oois"][decl["source"]]["scan_profile"]
            for decl in datamap["declarations"]
        ]
    )
    if res is not None:
        print(f"FAIL({inspect.currentframe().f_lineno}): {json.dumps(res, indent=2)}")
    time.sleep(1)
    res = noc.scan_profiles_recalculate()
    if res is not None:
        print(f"FAIL({inspect.currentframe().f_lineno}): {json.dumps(res, indent=2)}")
    objects = noc.objects()["items"]
    print(f"init: {len(objects)}")
    new_objects = objects
    counter = 0
    while new_objects or counter < 0xF:
        for obj in new_objects:
            for prototype in filter(
                lambda origin: origin["source"] == obj["primary_key"],
                datamap["observations"],
            ):
                origin = prototype.copy()
                origin["result"] = [datamap["oois"][pk] for pk in origin["result"]]
                res = noc.save_observation(origin)
                if res is not None:
                    print(
                        f"FAIL({inspect.currentframe().f_lineno}): {json.dumps(res, indent=2)}"
                    )
            for prototype in filter(
                lambda origin: origin["source"] == obj["primary_key"],
                datamap["affirmations"],
            ):
                origin = prototype.copy()
                res = noc.save_affirmations({"ooi": datamap["oois"][origin["source"]]})
                if res is not None:
                    print(
                        f"FAIL({inspect.currentframe().f_lineno}): {json.dumps(res, indent=2)}"
                    )
        res = noc.scan_profiles_recalculate()
        if res is not None:
            print(
                f"FAIL({inspect.currentframe().f_lineno}): {json.dumps(res, indent=2)}"
            )
        newer_objects = noc.objects()["items"]
        new_objects = [obj for obj in newer_objects if obj not in objects]
        objects = newer_objects.copy()
        print(f"{counter}: {len(objects)}")
        counter += 1
    objects = noc.objects()["items"]
    pks = [obj["primary_key"] for obj in objects]
    if len(datamap["oois"]) == len(objects):
        print(f"SUCCES: {len(datamap["oois"])}")
    else:
        print(f"FAIL: {len(datamap["oois"])} != {len(objects)}")
        if dump:
            for obj in datamap["oois"]:
                if obj not in pks:
                    print(obj)
    if xterminate:
        noc.node_delete(organisation)


if __name__ == "__main__":
    cli()
