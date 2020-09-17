COMMIT ?= $(shell git describe --tags --always --dirty)
BRANCH ?= $(shell git branch 2> /dev/null | sed -e '/^[^*]/d' -e 's/\* //' | tr '[:upper:]' '[:lower:]' | cut -c -63)


venv:
	python3.8 -m venv venv
	./venv/bin/pip --no-cache-dir install -r requirements.txt

.PHONY: clean
clean:
	rm -rf venv

.PHONY: flake8
flake8: venv
	./venv/bin/flake8 --exclude venv/

.PHONY: lint
lint: venv flake8

