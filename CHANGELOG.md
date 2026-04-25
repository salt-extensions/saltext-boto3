The changelog format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

This project uses [Semantic Versioning](https://semver.org/) - MAJOR.MINOR.PATCH

# Changelog

## 1.0.0 (2026-04-25)


### Removed

- Removed all legacy boto2 ``boto_*`` execution modules and states; use the ``boto3_*`` equivalents instead. [#boto2-legacy](https://github.com/salt-extensions/saltext-boto3/issues/boto2-legacy)


### Added

- Initial release of the ``saltext-boto3`` Salt Extension, carved out of Salt core and modernized to require boto3 1.28+/botocore 1.31+ on Python 3.10+. [#initial-extension](https://github.com/salt-extensions/saltext-boto3/issues/initial-extension)
