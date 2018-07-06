PYTHONENV  = PYTHONPATH=../csvdiff3
PYTHON     = python3
tests	   = headers_test.py file_test.py merge3_test.py

.PHONY: test
test: $(tests)

	> DEBUG.log

	for f in $(tests); do \
	  echo Running $$f: ; \
	  $(PYTHONENV) $(PYTHON) $$f ; \
	done

