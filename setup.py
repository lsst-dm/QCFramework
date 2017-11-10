from distutils.core import setup
import glob

# The main call
setup(name='QCFramework',
      version ='1.0.1',
      license = "GPL",
      description = "Code that monitors stdout/stderr for messages to store in DB",
      author = "Doug Friedel, Michelle Gower",
      author_email = "friedel@illinois.edu",
      packages = ['qcframework'],
      package_dir = {'': 'python'},
      data_files=[('ups',['ups/QCFramework.table']),]
      )

