.PHONY: validate test check ready
validate:
	python3 -B scripts/validate.py
test:
	python3 -B -m unittest discover -s tests/control -v
check: validate test
ready:
	python3 -B scripts/ready_tasks.py
