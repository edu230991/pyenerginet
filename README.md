# pyenerginet
A python package to facilitate using public data from Energinet.
Raw data available at https://www.energidataservice.dk/.

### Getting started
Clone repository:

```
git clone https://github.com/edu230991/pyenerginet.git
```

Create virtual environment (if you don't have `virtualenv`, just pip-install it) and activate it:

```
python -m virtualenv env
call env\Scripts\activate
```

Install dependencies (as well as local version of `pyenerginet`):

```
pip install -e .
```

To see whether the current package version works:
```
pip install pytest
python -m pytest
```

### Contributing 
Please feel free to request contributor access to edu230991.
Once you are a contributor, you will need to make a pull request and someone else will have to approve it before it is merged into main.

### Publishing
Set up following these guides:
- https://packaging.python.org/en/latest/tutorials/packaging-projects/
- https://packaging.python.org/en/latest/guides/publishing-package-distribution-releases-using-github-actions-ci-cd-workflows/

