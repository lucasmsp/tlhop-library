import sys
import setuptools
import tlhop


def get_requirements():
    """
    lists the requirements to install.
    """
    try:
        with open('requirements.txt') as f:
            requirements = f.read().splitlines()
    except FileExistsError as ex:
        requirements = []
    return requirements


def get_readme():
    try:
        with open('README.md') as f:
            readme = f.read()
    except FileExistsError as ex:
        readme = ''
    return readme


if sys.version_info < (3, 8):
    print("Python versions prior to 3.8 are not supported.",
          file=sys.stderr)
    sys.exit(-1)


VERSION = tlhop.__version__

setuptools.setup(
     name='tlhop-library',  
     version=VERSION,
     author="TLHOP Project Team",
     author_email="lucasmsp@dcc.ufmg.br",
     description="A library to process large volumes of Shodan data efficiently.",
     url="https://github.com/lucasmsp/tlhop-library",
     license='http://www.apache.org/licenses/LICENSE-2.0',
     platforms=['Linux'],
     long_description=get_readme(),
     long_description_content_type='text/markdown',
     include_package_data=True,
     classifiers=[
         'Programming Language :: Python :: 3',
         "Programming Language :: Python :: 3.8",
         'License :: OSI Approved :: Apache Software License',
         "Operating System :: POSIX :: Linux",
         "Topic :: Software Development :: Libraries",
         "Topic :: Software Development :: Libraries :: Python Modules",
         "Topic :: System :: Distributed Computing",
     ],
     packages=setuptools.find_packages(),
     install_requires=get_requirements(),

 )


