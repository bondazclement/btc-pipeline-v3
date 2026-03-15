"""
BTC Data Collection Pipeline v2
Temporal Fusion Transformer — Binary Options Pricing

v2: allégé — suppression des sources à faible utilité pour horizons 30s-5min
    (transactions, UTXO, BGeometrics, Blockchair).
    Corrections de bugs critiques (flush data loss, sigma vectorisation, state cache).
"""
__version__ = "2.0.0"
