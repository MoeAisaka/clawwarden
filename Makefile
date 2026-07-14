.PHONY: test scan preflight

test:
	python3 -m unittest discover -s tests -v
	node --test plugin/openclaw/tests/*.test.mjs

scan:
	python3 scripts/secret_scan.py .

preflight:
	./scripts/preflight.sh
