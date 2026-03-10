clean:
	rm -f db.sql backtest.json
	rm -f .coverage
	find . -type f -name '*.py[co]' -delete -o -type d -name __pycache__ -delete

fetch:
	python3 -m cex_arb.connector

backtest:
	python3 -m cex_arb.backtest

test:
	python3 -m pytest tests/ -v --tb=short -n auto --cov=cex_arb --cov-report=term-missing

plot:
	python3 -m cex_arb.plot
