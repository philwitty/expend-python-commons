from setuptools import setup, find_packages

setup(name='ex_py_commons',
      version='0.0.1',
      description='Expend python common libraries',
      url='https://github.com/curoo/expend-python-commons',
      author='Curoo Limited',
      author_email='dev@curoo.com',
      license='Copyright (c) 2015, Curoo Limited',
      packages=find_packages(exclude=['test*']),
      install_requires=[
        'boto3>=1.1.4',
        'ex_pb==0.0.1'
      ],
      dependency_links=[
        'git+https://github.com/curoo/expend-proto.git@38d25d4#egg=ex_pb-0.0.1'
      ],
      zip_safe=False)
