#!/usr/bin/env python

import inspect
import json
import random
import time

import click
import zstandard as zstd
from ascii_magic import AsciiArt
from dill import dumps, loads
from octopoes_client import OctopoesClient
from xxhash import xxh3_128_hexdigest as xxh3


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
        click.echo(AsciiArt.from_image("stresspoes.jpg").to_ascii())
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
    declarations_list = oc.load_bulk(
        [
            declarations["source"]
            for declarations in oc.origins(origin_type="declaration")
        ]
    )
    observation_mapping = {
        origin["source"]: {
            "origin": origin,
            "result_oois": oc.load_bulk(origin["result"]),
        }
        for origin in oc.origins(origin_type="observation")
        if origin["result"]
    }
    affirmations_list = {
        affirmation["source"]: oc.object(affirmation["source"])
        for affirmation in oc.origins(origin_type="affirmation")
    }
    datamap = {
        "organisation": ctx.obj["organisation"],
        "declarations_list": declarations_list,
        "observations_mapping": observation_mapping,
        "affirmations_list": affirmations_list,
        "objects": oc.objects(),
    }
    with open(filename, "wb") as file:
        payload = dumps(datamap)
        file.write(
            zstd.ZstdCompressor().compress(dumps((0xC0DECA7, payload, xxh3(payload))))
        )


@cli.command(
    help="Stress Octopoes (warning: this may destroy your OpenKAT installation)"
)
@click.option("-d", "--dump", is_flag=True, default=False, help="Dump opbjects diff")
@click.argument("filename", default="datamap.kat")
@click.pass_context
def stress(ctx: click.Context, filename: str, dump: bool):
    oc = ctx.obj["client"]
    with open(filename, "rb") as file:
        magic, datamap, checksum = loads(
            zstd.ZstdDecompressor().decompress(file.read())
        )
    if magic != 0xC0DECA7 or checksum != xxh3(datamap):
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
    noc.save_many_declarations(
        [{"ooi": decl} for _, decl in datamap["declarations_list"].items()]
    )
    time.sleep(1)
    objects = noc.objects()["items"]
    print(f"init: {len(objects)}")
    new_objects = objects
    counter = 0
    omp = datamap["observations_mapping"]
    amp = datamap["affirmations_list"]
    while new_objects or counter < 0xFF:
        if not new_objects:
            new_objects = objects
        for obj in new_objects:
            if obj["primary_key"] in omp:
                origin = omp[obj["primary_key"]]["origin"]
                origin["result"] = list(omp[obj["primary_key"]]["result_oois"].values())
                res = noc.save_observation(origin)
                if res is not None:
                    print(
                        f"fail({inspect.currentframe().f_lineno}): {json.dumps(res, indent=2)}"
                    )
            if obj["primary_key"] in amp:
                res = noc.save_affirmations({"ooi": amp[obj["primary_key"]]})
                if res is not None:
                    print(json.dumps(amp[obj["primary_key"]], indent=2))
                    print(
                        f"fail({inspect.currentframe().f_lineno}): {json.dumps(res, indent=2)}"
                    )
        # noc.bits_recalculate()
        newer_objects = noc.objects()["items"]
        new_objects = [obj for obj in newer_objects if obj not in objects]
        objects = newer_objects
        print(f"{counter}: {len(objects)}")
        counter += 1
    if len(datamap["objects"]["items"]) == len(objects):
        print(f"GOOOD {len(datamap["objects"]["items"])}")
        noc.node_delete(organisation)
    else:
        print(f"bleet: {len(datamap["objects"]["items"])} != {len(objects)}")
        if dump:
            for obj in datamap["objects"]["items"]:
                if obj not in objects:
                    print(obj["primary_key"])


if __name__ == "__main__":
    cli()
