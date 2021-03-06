# -*- coding: utf-8 -*-
#
# Copyright (C) 2015 Oliver Schulz <oschulz@mpp.mpg.de>
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from collections import namedtuple
import os
from distutils.spawn import find_executable

import luigi

from .logger import *
from .local_subprocess import *
from .tier_task import *
from .tier0 import *


class Tier1Gen(TierOptSystemTask):
    class Output(namedtuple('Output', ['tier1', 'tierX'])):
        __slots__ = ()


    def __init__(self, *args, **kwargs):
        super(Tier1Gen, self).__init__(*args, **kwargs)
        self.systems = [self.system] if self.system else self.gerda_config['proc']['tier1'].keys()


    def requires(self):
        return Tier0AvailKey(self.config, self.file_key)


    def run(self):
        logger.debug('Running Tier1GenSystem for "{key}", system "{system}"'.format(
            key = self.file_key, system = self.system))

        Consumer = namedtuple('Consumer', ['label', 'prog', 'args', 'out', 'log'])
        consumers = []

        Pipe = namedtuple('Pipe', ['rd', 'wr'])
        pipes = []

        try:
            tier0_in = self.input().data.open('r')

            producer_process = None
            if find_executable('pbzip2'):
                producer_process = LocalSubprocess(
                    label = '{key}_raw-decompress'.format(key = self.key.name),
                    program = 'pbzip2',
                    arguments = ['-d', '-c', '-p1', tier0_in.name],
                    stdin = None, stdout = subprocess.PIPE
                )
            elif find_executable('bzip2'):
                producer_process = LocalSubprocess(
                    label = '{key}_raw-decompress'.format(key = self.key.name),
                    program = 'bzip2',
                    arguments = ['-d', '-c', tier0_in.name],
                    stdin = None, stdout = subprocess.PIPE
                )
            else:
                raise RuntimeError('Need "pbzip2" ot "bzip2", but neither seems to be available.')

            raw2index_config = self.gerda_config['proc']['tierX']['all']['raw2index']
            tierX_conversion = ensure_str(raw2index_config['conversion'])

            for system in self.systems:
                raw2mgdo_config = self.gerda_config['proc']['tier1'][system]['raw2mgdo']
                conversion = ensure_str(raw2mgdo_config['conversion'])
                inverted = ensure_bool(raw2mgdo_config['inverted'])

                tier1_out = self.output().tier1[system].open('w')
                tier1_log = luigi.LocalTarget(self.gerda_data.log_file(self.file_key, system, 'tier1')).open('w')
                consumers.append( Consumer(
                    label = '{key}_{system}_Raw2MGDO'.format(key = self.key.name, system = system),
                    prog = 'Raw2MGDO',
                    args = ['-c', conversion, '-m', '50'] + (['--inverted'] if inverted else []) +
                        ['-f', tier1_out.name, 'stdin'],
                    out = tier1_out,
                    log = tier1_log,
                ) )

            tierX_out = self.output().tierX['all'].open('w')
            tierX_log = luigi.LocalTarget(self.gerda_data.log_file(self.file_key, 'all', 'tierX')).open('w')
            consumers.append( Consumer(
                label = '{key}_all_Raw2Index'.format(key = self.key.name),
                prog = 'Raw2Index',
                args = ['-c', conversion, '-f', tierX_out.name, 'stdin'],
                out = tierX_out,
                log = tierX_log
            ) )

            for consumer in consumers[:-1]:
                pipes.append(Pipe(*os.pipe()))

            tee_process = LocalSubprocess(
                label = '{key}_raw-stream-tee'.format(key = self.key.name),
                program = 'tee',
                arguments = ['/dev/fd/{}'.format(pipe.wr) for pipe in pipes],
                stdin = producer_process.stdout, stdout = subprocess.PIPE,
                pass_fds = [pipe.wr for pipe in pipes]
            )

            consumer_inputs = [pipe.rd for pipe in pipes] + [tee_process.stdout]
            consumer_processes = [LocalSubprocess(
                label = consumer.label,
                program = consumer.prog,
                arguments = consumer.args,
                stdin = input, stdout = consumer.log, stderr = subprocess.STDOUT
            ) for consumer, input in zip(consumers, consumer_inputs)]

            producer_process.wait_and_check(raise_exception = True)
            tee_process.wait_and_check(raise_exception = True)

            failed = [p for  p in consumer_processes if not p.wait_and_check(raise_exception = False)]
            if failed:
                raise RuntimeError("Some system tasks failed: {}".format([p.label for p in failed]))
            else:
                for consumer in consumers:
                    consumer.out.close()

        finally:
            for pipe in pipes:
                os.close(pipe.wr)
                os.close(pipe.rd)

            for consumer in consumers:
                consumer.log.close()


    def output(self):
        return Tier1Gen.Output(
            tier1 = { system: luigi.LocalTarget(self.gerda_data.data_file(self.file_key, system, 'tier1')) for system in self.systems },
            tierX = { 'all': luigi.LocalTarget(self.gerda_data.data_file(self.file_key, 'all', 'tierX')) }
        ) 
