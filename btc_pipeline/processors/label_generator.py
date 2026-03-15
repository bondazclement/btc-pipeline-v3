"""
Label Generator
Génère les labels pour le pricing d'options binaires BTC.

Labels pour chaque timestep t :
  - mu_30s : drift (return) réalisé sur les 30 prochaines secondes
  - mu_1m  : drift réalisé sur la prochaine minute
  - mu_3m  : drift réalisé sur les 3 prochaines minutes
  - mu_5m  : drift réalisé sur les 5 prochaines minutes
  - sigma_30s : volatilité réalisée (std des returns 1s) sur 30s forward
  - sigma_1m  : volatilité réalisée sur 1m forward
  - sigma_3m  : volatilité réalisée sur 3m forward
  - sigma_5m  : volatilité réalisée sur 5m forward
  - direction_30s : 1 si price[t+30] > price[t], 0 sinon
  - direction_1m  : idem 1 minute
  - direction_3m  : idem 3 minutes
  - direction_5m  : idem 5 minutes

ATTENTION : Les labels utilisent des données FUTURES.
Ils ne doivent JAMAIS être inclus comme features (data leakage).
"""

import numpy as np
from numpy.lib.stride_tricks import sliding_window_view
import pandas as pd
from loguru import logger


HORIZONS = {
    "30s": 30,
    "1m": 60,
    "3m": 180,
    "5m": 300,
}


def _compute_sigma_vectorized(returns: np.ndarray, horizon: int) -> np.ndarray:
    """
    Calcule la volatilité réalisée forward (std des returns 1s) sur `horizon` pas, vectorisé.

    v2: remplace la boucle Python O(n*horizon) par numpy sliding_window_view.
    Pour 250M lignes × 300 secondes, passe de dizaines d'heures à quelques minutes.
    """
    # On veut std(returns[i+1 : i+1+horizon]) pour chaque i
    if len(returns) <= horizon:
        return np.full(len(returns), np.nan)

    # Fenêtre glissante sur returns[1:] pour obtenir les fenêtres forward
    forward_returns = returns[1:]  # décalage de 1 pour exclure le return courant
    if len(forward_returns) < horizon:
        return np.full(len(returns), np.nan)

    windows = sliding_window_view(forward_returns, horizon)
    sigma = windows.std(axis=1)

    # Aligner : sigma[i] correspond à std(returns[i+1:i+1+horizon])
    result = np.full(len(returns), np.nan)
    result[:len(sigma)] = sigma
    return result


def generate_labels(df: pd.DataFrame) -> pd.DataFrame:
    """
    Génère les labels à partir du DataFrame 1s (doit contenir 'close' et 'timestamp_s').
    Les labels sont les returns et volatilités réalisés sur les horizons forward.

    IMPORTANT : Les N dernières lignes auront des NaN (pas de futur disponible).
    Ces lignes doivent être exclues du training set.
    """
    if "close" not in df.columns:
        raise ValueError("DataFrame must contain 'close' column")

    labels = pd.DataFrame({"timestamp_s": df["timestamp_s"]})

    close = df["close"].values
    returns_1s = np.diff(close, prepend=close[0]) / np.maximum(close, 1e-10)

    for name, horizon in HORIZONS.items():
        # ── Drift (mu) : log-return sur l'horizon ─────────────────────────
        future_close = pd.Series(close).shift(-horizon)
        mu = np.log(future_close / pd.Series(close).replace(0, np.nan))
        labels[f"mu_{name}"] = mu.values

        # ── Volatilité réalisée (sigma) : std des returns 1s sur l'horizon ─
        # v2: Bug 2 fix — vectorisé avec sliding_window_view au lieu d'une boucle Python O(n*horizon)
        sigma_vals = _compute_sigma_vectorized(returns_1s, horizon)
        labels[f"sigma_{name}"] = sigma_vals

        # ── Direction : 1 si hausse, 0 si baisse ─────────────────────────
        direction = (future_close > pd.Series(close)).astype("float32")
        labels[f"direction_{name}"] = direction.values

    # Count valid labels (non-NaN)
    valid_rows = labels.dropna().shape[0]
    total_rows = len(labels)
    logger.info(
        f"Labels generated: {total_rows:,} rows, "
        f"{valid_rows:,} valid ({valid_rows/total_rows*100:.1f}%)"
    )

    return labels


def validate_no_leakage(df_features: pd.DataFrame, df_labels: pd.DataFrame) -> bool:
    """
    Vérifie qu'aucune colonne du DataFrame features ne contient d'information future.
    Test simple : les features au temps t ne doivent pas être corrélées avec les labels
    au temps t plus que les features au temps t-1 ne le sont avec les labels au temps t.

    Retourne True si pas de leakage détecté.
    """
    if len(df_features) < 1000:
        logger.warning("Too few rows for leakage test")
        return True

    # Sample for speed
    n = min(10000, len(df_features))
    idx = np.random.choice(len(df_features), n, replace=False)
    idx = sorted(idx)

    features_sample = df_features.iloc[idx].select_dtypes(include=[np.number])
    labels_sample = df_labels.iloc[idx]

    # Check: features at time t should not be more correlated with labels at t
    # than features at t-1 are with labels at t
    label_col = "mu_1m"
    if label_col not in labels_sample.columns:
        label_col = labels_sample.columns[1] if len(labels_sample.columns) > 1 else None

    if label_col is None:
        return True

    y = labels_sample[label_col].dropna()
    common_idx = features_sample.index.intersection(y.index)

    if len(common_idx) < 100:
        return True

    # Correlation of features[t] with label[t]
    corr_t = features_sample.loc[common_idx].corrwith(y.loc[common_idx]).abs()

    # Correlation of features[t-1] with label[t]
    shifted = features_sample.shift(1).loc[common_idx]
    corr_t1 = shifted.corrwith(y.loc[common_idx]).abs()

    # If feature[t] is MUCH more correlated than feature[t-1], suspect leakage
    suspicious = (corr_t > 0.5) & (corr_t > corr_t1 * 2)
    suspicious_cols = suspicious[suspicious].index.tolist()

    if suspicious_cols:
        logger.error(f"⚠️ Potential data leakage detected in columns: {suspicious_cols}")
        return False

    logger.info("✅ No data leakage detected")
    return True
