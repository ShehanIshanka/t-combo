from setuptools import setup, find_packages

setup(
    name='t_combo',
    version='1.0.0',
    description='Utility tool for combining texts',
    url='https://github.com/ShehanIshanka/t-combo',
    author='Shehan Ishanka',
    author_email='ishanka.shehan@gmail.com',
    license='MIT',
    download_url = 'https://github.com/ShehanIshanka/t-combo/tarball/1.0.0',
    keywords = ['Text Combo', 'T Combo'],
    packages=find_packages(exclude=['test'])
)