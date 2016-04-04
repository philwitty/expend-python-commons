FLAKE=flake8

review:
	@$(FLAKE) --max-complexity 10 .
.PHONY: review
