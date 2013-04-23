#from distutils.core import setup
from setuptools import setup
import os 


def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()


setup(
    name='DynamodbFdw',
    version='0.1.1',
    author='Fabio Rueda',
    author_email='avances123@gmail.com',
    packages=['dynamodbfdw'],
    url='https://github.com/avances123/dynamodb_fdw',
    license='LICENSE.txt',
    description='Postgresql Foregin Data Wrapper mapping Amazon DynamoDB',
    #long_description=open('README.txt').read(),
    #long_description=read('README.txt'),
    install_requires=["boto"],
)
