clean-pyc:
	find . -name "*.pyo" -exec rm -f "{}" \;
	find . -name "*.pyc" -exec rm -f "{}" \;

clean-build:
	rm -f MANIFEST
	rm -rf build/
	rm -rf dist/
	rm -rf *.egg-info

sdist-upload:
	python setup.py sdist
	source .env ; twine upload dist/* --skip-existing


upload: clean-build sdist-upload clean-build clean-pyc


bump-patch:
	bumpversion patch

bump-minor:
	bumpversion minor

