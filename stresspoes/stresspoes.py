#!/usr/bin/env python

import inspect
import json
import random
import re
import time
from copy import deepcopy
from functools import reduce
from typing import Any

import click
import zstandard as zstd
from dill import dumps, loads
from octopoes_client import OctopoesClient
from term_image.image import from_file
from xxhash import xxh3_128_hexdigest as xxh3
from rabbitmq_size import get_event_count

MAGIC = 0xC0DECA7


def merge_dicts(d1: dict[str, Any], d2: dict[str, Any]) -> dict[str, Any]:
    return {
        key: {**value, **d2[key]} if isinstance(value, dict) else value + d2[key]
        for key, value in d1.items()
    }


def seek(d: dict[str, Any], key: str, regex: str) -> str | None:
    if key in d and isinstance(d[key], str):
        if re.search(regex, d[key]):
            return d[key]
    for v in d.values():
        if isinstance(v, dict):
            retval = seek(v, key, regex)
            if retval is not None:
                return retval
    return None


def replace(d: dict[str, Any], source: str, target: str) -> dict[str, Any]:
    return json.loads(json.dumps(d).replace(source, target))


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


@cli.command(help="Dump Datamap")
@click.argument("filename", default="datamap.kat")
@click.pass_context
def dump(ctx: click.Context, filename: str):
    with open(filename, "rb") as file:
        magic, datamap, checksum = loads(
            zstd.ZstdDecompressor().decompress(file.read())
        )
    if magic != MAGIC or checksum != xxh3(datamap):
        click.echo(f"Datamap file {filename} seems corrupted.")
        return
    else:
        datamap = loads(datamap)
    click.echo(json.dumps(datamap, indent=2))


@cli.command(
    help="Stress Octopoes (warning: this may destroy your OpenKAT installation)"
)
@click.option("-d", "--dump", is_flag=True, default=False, help="Dump opbjects diff")
@click.option(
    "-x",
    "--noxterminate",
    is_flag=True,
    default=True,
    help="Delete resulting organisation",
)
@click.option(
    "-m",
    "--multiplier",
    default=1,
    help="Multiply base objects by a value",
)
@click.option("-t", "--threshold", default=0xF, help="Number of rounds after nulling")
@click.option("-o", "--timeout", default=0.0, help="Relax the round")
@click.argument("filename", default="datamap.kat")
@click.pass_context
def stress(
    ctx: click.Context,
    filename: str,
    dump: bool,
    noxterminate: bool,
    multiplier: int,
    threshold: int,
    timeout: float,
):
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
    if multiplier > 1:
        target = seek(datamap, "primary_key", r"^Network\|[^|]+$")
        if target is not None:
            target = target.split("|")[-1]
            enriched = [
                replace(deepcopy(datamap), target, f"{target}-{i}")
                for i in range(multiplier - 1)
            ]
            datamap = merge_dicts(datamap, reduce(merge_dicts, enriched))
            datamap["organisation"] = oc.org
    print(f"declarations: {len(datamap["declarations"])}")
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
    relaxer = 0
    events = 1
    times = []
    operations = []
    while new_objects or relaxer < threshold or events > 0:
        ops = 1
        begin = time.perf_counter_ns()
        for obj in new_objects:
            for prototype in filter(
                lambda origin: origin["source"] == obj["primary_key"],
                datamap["observations"],
            ):
                origin = deepcopy(prototype)
                origin["result"] = [datamap["oois"][pk] for pk in origin["result"]]
                res = noc.save_observation(origin)
                if res is not None:
                    print(
                        f"FAIL({inspect.currentframe().f_lineno}): {json.dumps(res, indent=2)}"
                    )
                ops += 1
            for prototype in filter(
                lambda origin: origin["source"] == obj["primary_key"],
                datamap["affirmations"],
            ):
                origin = deepcopy(prototype)
                res = noc.save_affirmations({"ooi": datamap["oois"][origin["source"]]})
                if res is not None:
                    print(
                        f"FAIL({inspect.currentframe().f_lineno}): {json.dumps(res, indent=2)}"
                    )
                ops += 1
        timediff = (time.perf_counter_ns() - begin) / 10e9
        times.append(timediff)
        operations.append(ops)
        res = noc.scan_profiles_recalculate()
        if res is not None:
            print(
                f"FAIL({inspect.currentframe().f_lineno}): {json.dumps(res, indent=2)}"
            )
        newer_objects = noc.objects()["items"]
        new_objects = [obj for obj in newer_objects if obj not in objects]
        if len(new_objects) == 0:
            relaxer += 1
        else:
            relaxer = 0
        objects = deepcopy(newer_objects)
        events = get_event_count()
        print(f"{counter}: {len(objects)} ({ops}/{events}: {timediff}s)")
        time.sleep(timeout)
        counter += 1
    objects = noc.objects()["items"]
    pks = [obj["primary_key"] for obj in objects]
    if len(datamap["oois"]) == len(objects) and all(
        ref in pks for ref in datamap["oois"]
    ):
        print(f"SUCCES: {len(datamap["oois"])} in ({sum(operations)}: {sum(times)})s")
    else:
        print(
            f"FAIL: {len(datamap["oois"])} ({datamap["organisation"]}) != {len(objects)} ({organisation}) in ({sum(operations)}: {sum(times)}s)"
        )
        if dump:
            counter = 0
            for obj in datamap["oois"]:
                if obj not in pks:
                    print(f"--> {obj}")
                    counter += 1
            if counter > 0:
                print(f"diff: {counter}")
            counter = 0
            for obj in pks:
                if obj not in datamap["oois"]:
                    print(f"<-- {obj}")
                    counter += 1
            if counter > 0:
                print(f"diff: {counter}")
    if noxterminate:
        noc.node_delete(organisation)


if __name__ == "__main__":
    cli()
