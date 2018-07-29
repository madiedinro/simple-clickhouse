clean-pyc:
	find . -name '*.pyc' -exec rm --force {} +
	find . -name '*.pyo' -exec rm --force {} +
	find . -name '*~' -exec rm --force  {} +

clean-build:
	rm -f MANIFEST
	rm -rf build/
	rm -rf dist/
	rm -rf *.egg-info

upload:
	python setup.py sdist bdist_wheel upload
	make clean-build
