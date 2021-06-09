.PHONY: test
test:
	@coverage run -m --branch --source=src,tests pytest tests


.PHONY: testcov
testcov: test
	@coverage html --precision=2 --skip-covered && x-www-browser htmlcov/index.html

.PHONY: lint
lint:
	@flake8 --max-line-length 88 src lambda_function.py tests

.PHONY: clean
clean:
	@-python3 -Bc "for p in __import__('pathlib').Path('.').rglob('*.py[co]'): p.unlink()"
	@-python3 -Bc "for p in __import__('pathlib').Path('.').rglob('__pycache__'): p.rmdir()"
	@-rm -rf .pytest_cache
	@-rm -rf htmlcov
	@-rm .coverage
