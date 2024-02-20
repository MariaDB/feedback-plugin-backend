from abc import ABC, abstractmethod

from collections import defaultdict
import inspect
import re
import sys
import json


class DataExtractor(ABC):
    @abstractmethod
    def get_required_keys(self) -> set[str]:
        '''
            Returns a list of Data keys that this data extractor
            needs to look at in order to extract Facts.
        '''
        pass


class UploadFactExtractor(DataExtractor):
    @abstractmethod
    def extract_facts(self,
                      data_dict: dict[int, dict[int, dict[str, list[str]]]]
                      ) -> dict[int, dict[int, dict[str, str]]]:
        '''
        Data_dict is a dictionary of the form:
        { <server_id> : {
            <upload_id> : {
                <upload_data_key> : [<values_for_upload_key>, ...] }}}

        Returns a dictionary of the form:
        {
            <server_id1>:
                <upload_id1>: {
                    <fact_key1>: <fact_value1>,
                    ...
                    },
                ...
                },
            ...
        }
        '''
        pass


class ServerFactExtractor(DataExtractor):
    @abstractmethod
    def extract_facts(self,
                      data_dict: dict[int, dict[int, dict[str, list[str]]]]
                      ) -> dict[int, dict[str, str]]:
        '''
        Data_dict is a dictionary of the form:
        { <server_id> : {
            <upload_id> : {
                <upload_data_key> : [<values_for_upload_key>, ...] }}}

        Returns a dictionary of the form:
        {
            <server_id1>: {
                <fact_key1>: <fact_value1>,
                ...
                },
                ...
            },
            ...
        }
        '''
        pass


class ArchitectureExtractor(ServerFactExtractor):
    def get_required_keys(self) -> set[str]:
        return {'uname_machine', 'uname_sysname', 'uname_version',
                'uname_distribution', 'uname_release'}

    @staticmethod
    def extract_distribution(upload: dict[str, list[str]]) -> str:
        if 'uname_distribution' not in upload:
            return None

        distro_string = upload['uname_distribution'][-1].lower()
        distro_keywords = [
            (['archlinux'], 'ArchLinux'),
            (['centos', 'rhel'], 'CentOS'),
            (['fedora'], 'Fedora'),
            (['gentoo'], 'Gentoo'),
            (['mint'], 'Linux Mint'),
            (['redhat', 'rhel'], 'Red Hat Enterprise Linux'),
            (['ubuntu'], 'Ubuntu'),
            # TODO: fill in more names that need to be cleaned up.
        ]

        for (patterns, pretty_form) in distro_keywords:
            for pattern in patterns:
                if pattern in distro_string:
                    return pretty_form
        return distro_string

    @staticmethod
    def extract_machine_architecture(upload: dict[str, list[str]]) -> str:
        if 'uname_machine' not in upload:
            return None

        machine = upload['uname_machine'][-1].lower()
        if re.match('^(x(86_)?64)|(amd64)$', machine):
            machine_architecture = 'x86_64'
        elif re.match('^[ix][3-6]*86$', machine):
            # This check must happen after x86_64
            machine_architecture = 'x86'
        elif re.match('^(aarch64|arm64)$', machine):
            machine_architecture = 'armv8'
        elif re.match('^hp_', machine):
            machine_architecture = 'HP Itanium'
        elif re.match('^alpha', machine):
            machine_architecture = 'Alpha'
        elif re.match('^mips$', machine):
            machine_architecture = 'MIPS'
        else:
            machine_architecture = machine

        return machine_architecture

    @staticmethod
    def extract_operating_system(upload: dict[str, list[str]]) -> str:
        if 'uname_sysname' not in upload:
            return None

        sysname = upload['uname_sysname'][-1].lower()
        if 'linux' in sysname:
            operating_system = 'Linux'
        elif 'windows' in sysname:
            operating_system = 'Windows'
        elif 'freebsd' in sysname:
            operating_system = 'FreeBSD'
        elif 'darwin' in sysname:
            operating_system = 'OSX'
        else:
            operating_system = 'unknown'

        return operating_system

    @staticmethod
    def extract_os_version(upload: dict[str, list[str]]) -> str:
        if 'uname_version' not in upload:
            return None

        version_string = upload['uname_version'][-1].lower()

        # TODO(cvicentiu): Hack to clean up version strings. This will need
        # to be changed to cover and extract a wide range of data points.
        if 'smp' in version_string:
            version_string = 'unknown'
            try:
                version_string = 'unknown'
                distro_string = ''
                if 'uname_distribution' in upload:
                    distro_string = upload['uname_distribution'][-1].lower()

                # Crude expression for CentOS 8
                if 'linux release' in distro_string:
                    first_digit = re.search('[0-9]+', distro_string)
                    if first_digit is not None:
                        version_string = distro_string[first_digit.start():]
            except KeyError:
                pass

        return version_string

    def extract_facts(self,
                      data_dict: dict[int, dict[int, dict[str, list[str]]]]
                      ) -> dict[int, dict[str, str]]:
        result = {}

        for server_id, server_uploads in data_dict.items():
            facts = {}
            for upload_id, upload in server_uploads.items():
                fact = ArchitectureExtractor.extract_operating_system(upload)
            if fact is not None:
                facts['operating_system'] = fact

            fact = ArchitectureExtractor.extract_machine_architecture(upload)
            if fact is not None:
                facts['hardware_architecture'] = fact

            fact = ArchitectureExtractor.extract_distribution(upload)
            if fact is not None:
                facts['distribution'] = fact

            fact = ArchitectureExtractor.extract_os_version(upload)
            if fact is not None:
                facts['operating_system_version'] = fact

            result[server_id] = facts
        return result


class ServerVersionExtractor(UploadFactExtractor):
    @staticmethod
    def extract_server_version(upload: dict[str, list[str]]) -> dict[str, str]:
        # Version key not present or its present with NULL values.
        # TODO(cvicentiu): Can upload['version'] actually be an empty list?
        if 'version' not in upload or len(upload['version']) == 0:
            return None
        pattern = re.compile('(?P<major>\\d+).(?P<minor>\\d+).(?P<point>\\d+)')

        # We always take the last entry from a CSV if there happen to be
        # duplicates.
        matches = pattern.match(upload['version'][-1])

        # TODO(cvicentiu) Matches set to None means regex missmatch.
        # Create a test case for this.
        if matches is None:
            return None

        result = {
            'server_version_major': matches.group('major'),
            'server_version_minor': matches.group('minor'),
            'server_version_point': matches.group('point'),
        }

        return result

    def get_required_keys(self) -> set[str]:
        return {'version'}

    def extract_facts(self,
                      data_dict: dict[int, dict[int, dict[str, list[str]]]]
                      ) -> dict[int, dict[int, dict[str, str]]]:
        result = {}
        for server_id, server_uploads in data_dict.items():
            facts = {}
            for upload_id, upload in server_uploads.items():
                fact = ServerVersionExtractor.extract_server_version(upload)
                if fact is None:
                    continue
                facts[upload_id] = fact
            result[server_id] = facts
        return result


COLLECTED_FEATURES = {'check_constraint', 'json', 'subquery', 'timezone'}
'''The set of features collected by the extractor'''

class ServerFeatureExtractor(UploadFactExtractor):
    @staticmethod
    def extract_features(upload: dict[str, list[str]]) -> dict[str, bool]:
        result: dict[str, bool] = {}
        for feature in COLLECTED_FEATURES:
            if values := upload.get('feature_' + feature):
                if any(value != "0" for value in values):
                    result[feature] = True
        return result

    def get_required_keys(self) -> set[str]:
        return {'feature_' + feature for feature in COLLECTED_FEATURES}

    def extract_facts(self,
                      data_dict: dict[int, dict[int, dict[str, list[str]]]]
                      ) -> dict[int, dict[int, dict[str, str]]]:
        result = {}
        for server_id, server_uploads in data_dict.items():
            facts: dict[int, dict[str, str]] = {}
            for upload_id, upload in server_uploads.items():
                fact = ServerFeatureExtractor.extract_features(upload)
                if not fact:
                    continue
                facts[upload_id] = {"features": json.dumps(fact)}
            result[server_id] = facts
        return result


def combine_server_facts(factset: list[dict[int, dict[str, str]]]) -> dict[int, dict[str, str]]:
    '''Combine the provided server facts into a single fact dictionary.'''

    result = defaultdict(dict)
    for facts in factset:
        for s_id in facts:
            result[s_id].update(facts[s_id])
    return result


def combine_upload_facts(factset: list[dict[int, dict[int, dict[str, str]]]]) -> dict[int, dict[int, dict[str, str]]]:
    '''Combine the provided upload facts into a single fact dictionary.'''

    result = defaultdict[int, dict](lambda: defaultdict(dict))
    for facts in factset:
        for server_id, uploads in facts.items():
            for upload_id, fields in uploads.items():
                result[server_id][upload_id].update(fields)
    return result


class AllFactExtractor(DataExtractor):
    def __init__(self, class_type: type):
        def class_filter(member):
            if (inspect.isclass(member)
                    and issubclass(member, class_type)
                    and member is not class_type
                    and not issubclass(member, AllFactExtractor)):
                return True
            return False

        cls_members = inspect.getmembers(sys.modules[__name__], class_filter)

        self.extractors = []
        for cls_member in cls_members:
            self.extractors.append(cls_member[1]())

    def get_required_keys(self):
        result = set()
        for extractor in self.extractors:
            result |= extractor.get_required_keys()
        return result


class AllUploadFactExtractor(AllFactExtractor, UploadFactExtractor):
    def __init__(self):
        super().__init__(UploadFactExtractor)

    def extract_facts(self, data_dict) -> dict[int, dict[int, dict[str, str]]]:
        return combine_upload_facts(
            [extractor.extract_facts(data_dict) for extractor in self.extractors]
        )


class AllServerFactExtractor(AllFactExtractor, ServerFactExtractor):
    def __init__(self):
        super().__init__(ServerFactExtractor)

    def extract_facts(self, data_dict) -> dict[int, dict[str, str]]:
        return combine_server_facts(
            [extractor.extract_facts(data_dict) for extractor in self.extractors]
        )
