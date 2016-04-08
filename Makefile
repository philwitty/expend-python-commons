FLAKE=flake8
PIP=pip3
NOSETESTS=nose2

review:
	@$(FLAKE) --max-complexity 10 .
.PHONY: review

deps:
	@$(PIP) install --upgrade -r requirements.txt
.PHONY: deps

test:
	@$(NOSETESTS) -v --with-coverage tests
.PHONY: test
