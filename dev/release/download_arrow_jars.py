#!/usr/bin/env python
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import re

import argparse
import concurrent.futures as cf
import functools
import os
import subprocess
import urllib.request


CROOSBOW_RELEASE = "https://api.github.com/repos/ursacomputing/crossbow/releases/tags/{}"
DEFAULT_PARALLEL_DOWNLOADS = 8


class Artifactory:

    def get_file_list(self, prefix):
        def traverse(directory, files, directories):
            url = f'{CROOSBOW_RELEASE}/{directory}'
            response = urllib.request.urlopen(url).read().decode()
            paths = re.findall('<a href="(.+?)"', response)
            for path in paths:
                if path == '../':
                    continue
                resolved_path = f'{directory}{path}'
                if path.endswith('/'):
                    directories.append(resolved_path)
                else:
                    files.append(resolved_path)
        files = []
        if not prefix.endswith('/'):
            prefix += '/'
        directories = [prefix]
        while len(directories) > 0:
            directory = directories.pop()
            traverse(directory, files, directories)
        return files

    def download_files(self, files, dest=None, num_parallel=None,
                       re_match=None):
        """
        Download files from Bintray in parallel. If file already exists, will
        overwrite if the checksum does not match what Bintray says it should be

        Parameters
        ----------
        files : List[Dict]
            File listing from Bintray
        dest : str, default None
            Defaults to current working directory
        num_parallel : int, default 8
            Number of files to download in parallel. If set to None, uses
            default
        """
        if dest is None:
            dest = os.getcwd()
        if num_parallel is None:
            num_parallel = DEFAULT_PARALLEL_DOWNLOADS

        if re_match is not None:
            regex = re.compile(re_match)
            files = [x for x in files if regex.match(x)]

        if num_parallel == 1:
            for path in files:
                self._download_file(dest, path)
        else:
            parallel_map_terminate_early(
                functools.partial(self._download_file, dest),
                files,
                num_parallel
            )

    def _download_file(self, dest, path):
        base, filename = os.path.split(path)

        dest_dir = os.path.join(dest, base)
        os.makedirs(dest_dir, exist_ok=True)

        dest_path = os.path.join(dest_dir, filename)

        print("Downloading {} to {}".format(path, dest_path))

        url = f'{CROOSBOW_RELEASE}/{path}'

        cmd = [
            'curl', '--fail', '--location', '--retry', '5',
            '--output', dest_path, url
        ]
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE)
        stdout, stderr = proc.communicate()
        if proc.returncode != 0:
            raise Exception("Downloading {} failed\nstdout: {}\nstderr: {}"
                            .format(path, stdout, stderr))


def parallel_map_terminate_early(f, iterable, num_parallel):
    tasks = []
    with cf.ProcessPoolExecutor(num_parallel) as pool:
        for v in iterable:
            tasks.append(pool.submit(functools.partial(f, v)))

        for task in cf.as_completed(tasks):
            if task.exception() is not None:
                e = task.exception()
                for task in tasks:
                    task.cancel()
                raise e


def download_jars(version, re_match=None, dest=None,
                  num_parallel=None, target_package_type=None):
    artifactory = Artifactory()

    artifactory.download_files(re_match=re_match, dest=dest,
                               num_parallel=num_parallel)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Download the arrow jars'
    )
    parser.add_argument('version', type=str, help='The version number')
    parser.add_argument('-e', '--regexp', type=str, default=None,
                        help=('Regular expression to match on file names '
                              'to only download certain files'))
    parser.add_argument('--dest', type=str, default=os.getcwd(),
                        help='The output folder for the downloaded files')
    parser.add_argument('--num_parallel', type=int, default=8,
                        help='The number of concurrent downloads to do')
    args = parser.parse_args()

    download_jars(args.version, args.rc_number, dest=args.dest,
                  re_match=args.regexp, num_parallel=args.num_parallel)
