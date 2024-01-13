# beancount-importers-india
A collection of beancount importers for indian banks

* For more information on how to use these, please checkout beancount project on github
* refer [example config](./example_config.py) on how to load these importers.

# Installation
* Install this package with ```pip3 install git+https://github.com/dumbPy/beancount-importers-india.git```
* Also install the required apt packages (or equivalent in non-debian based os) mentioned in [apt_requirements.txt](./apt_requirements.txt)

### Extracting pdf tables
* some pdf formats work great with tabula while others work best with camelot-py
* If you need to create a template for table extraction using excalibur-py and it's [docker image](https://hub.docker.com/r/williamjackson/excalibur)

### Future Work
* Would love to support statements from all the banks in India.
* Share an example pdf/excel/csv/docx of a statement with me if you would like me to support some more banks.
