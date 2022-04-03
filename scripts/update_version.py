#!/usr/bin/env python3

import re
import sys
import subprocess

VERSION_MATCHER = re.compile(r'^version = "([^"]+)\.([^"]+)\.([^"]+)"$')

def run(args, cwd="."):
    print("%s => %s" % (cwd, args))
    subprocess.check_call(args, cwd=cwd)

def update_version(path, new_version):
    with open(path, "r") as f:
        contents = f.read().splitlines()

    in_package = False

    for i, line in enumerate(contents):
        if line.startswith("["):
            in_package = line == "[package]"
        elif in_package:
            match = VERSION_MATCHER.match(line)

            if match:
                old_version = tuple([int(x) for x in match.groups()])
                assert new_version > old_version, "New version should be greater than the old version"
                contents[i] = 'version = "{}"'.format(".".join(str(x) for x in new_version))

    with open(path, "w") as f:
        f.write("\n".join(contents))

def main():
    if len(sys.argv) < 2:
        raise Exception("No version specified")

    try:
        new_version = tuple([int(x) for x in sys.argv[1].split(".")])
        assert len(new_version) == 3
    except:
        raise Exception("Invalid version specification")

    update_version("Cargo.toml", new_version)

    run(["make", "check", "test"])

    # a github action will pickup this tag push and create a release
    new_version_str = "v{}".format(".".join(str(x) for x in new_version))
    run(["git", "commit", "-a", "-m", new_version_str])
    run(["git", "tag", "-a", new_version_str, "-m", new_version_str])
    run(["git", "push", "origin", new_version_str])
    run(["cargo", "publish"])

if __name__ == "__main__":
    main()
