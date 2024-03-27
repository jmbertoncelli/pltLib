# pltLib
library wrapper for StreamSets SDK
- install cryptography & streamsets
- conda export --no-builds -n pltLib > environment.yaml
- conda install -c conda-forge grayskull
- grayskull pypi https://github.com/jmbertoncelli/pltLib
- grayskull pypi https://github.com/jmbertoncelli/pltLib --extras-require-all --extras-require-split
- conda config --set anaconda_upload yes
- conda config --set report_errors false
- conda build -c jmbertoncelli pltLib