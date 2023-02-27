VENV = .venv
PYTHON = $(VENV)/bin/python
PIP = $(VENV)/bin/pip
TEST = pytest

$(VENV)/bin/activate: requirements.txt
	python3 -m venv .venv
	$(PIP) install -r requirements.txt

clean:
	rm -rf __pycache__
	rm -rf tests/__pycache__
	rm -rf app/__pycache__
	rm -rf .pytest_cache
	rm -rf .ruff_cache
	rm -rf logs
	rm -rf *.xlsx

update: $(VENV)/bin/activate
	$(PIP) install -U pip
	$(PYTHON) -m pre_commit autoupdate

lint: $(VENV)/bin/activate
	$(PYTHON) -m pre_commit install --install-hooks
	$(PYTHON) -m pre_commit run --all-files
