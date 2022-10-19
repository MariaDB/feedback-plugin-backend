import re

class DataExtractor(object):
  def extract_facts(data_dict):
    result = []
    return result


class ArchitectureExtractor(DataExtractor):

  def get_required_keys(self):
    return ['uname_machine', 'uname_sysname', 'uname_version',
            'uname_distribution', 'uname_release']

  @staticmethod
  def extract_distribution(upload):
    if 'uname_distribution' not in upload:
      return None

    distro_string = upload['uname_distribution'][0].lower()
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
  def extract_machine_architecture(upload):
    if 'uname_machine' not in upload:
      return None

    machine = upload['uname_machine'][0].lower()
    if re.match('^(x(86_)?64)|(amd64)$', machine):
      machine_architecture = 'x86_64'
    elif re.match('^[ix][3-6]*86$', machine): # This check must happen after x86_64
      machine_architecture = 'x86'
    elif re.match('^armv[5-7]', machine):
      machine_architecture = 'ARM 32Bit'
    elif re.match('^aarch64$', machine):
      machine_architecture = 'ARM 64Bit'
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
  def extract_operating_system(upload):
    if 'uname_sysname' not in upload:
      return None

    sysname = upload['uname_sysname'][0].lower()
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

  def extract_os_version(upload):
    if 'uname_version' not in upload:
      return None

    version_string = upload['uname_version'][0].lower()
    #TODO(cvicentiu): Hack to clean up version strings. This will need
    # to be changed to cover and extract a wide range of data points.
    if 'smp' in version_string:
      version_string = 'unknown'
      try:
        version_string = 'unknown'
        distribution_string = ''
        if 'uname_distribution' in upload:
          distribution_string = upload['uname_distribution'][0].lower()

        # Crude expression for CentOS 8
        if 'linux release' in distribution_string:
          first_digit = re.search('[0-9]+', distribution_string)
          if first_digit is not None:
            version_string = distribution_string[first_digit.start():]

      except KeyError:
        pass

    return version_string

  def extract_facts(self, servers):
    result = {}

    for server_id, server_uploads in servers.items():
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
