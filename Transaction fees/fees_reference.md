# Frais de trading — référence pour backtesteur

Dernière vérification : 27 septembre 2026. Sources : pages officielles des exchanges
(docs.dydx.exchange, hyperliquid.gitbook.io, fee schedules Binance/Bybit/OKX/KuCoin).
Les frais changent régulièrement (annonces fréquentes de la part des CEX) — à revalider
avant tout déploiement en production.

**Convention** : tous les taux sont en % du notionnel (prix × quantité), pas de la marge.
Un maker fee négatif = rebate (l'exchange te paie).

---

## 1. Binance

### Spot
| Tier | Volume 30j (BTC ou équiv.) | Maker | Taker |
|---|---|---|---|
| Regular (VIP0) | < 1M$ | 0,1000% | 0,1000% |
| Regular + BNB discount | — | 0,0750% | 0,0750% |
| VIP1 | ≥ 1M$ | 0,0900% | 0,1000% |
| VIP5 | ≥ 100M$ | 0,0350% | 0,0600% |
| VIP9 | ≥ 4B$ | 0,0120% | 0,0240% |

### USDT-M Futures (perp)
| Tier | Maker | Taker |
|---|---|---|
| Regular (VIP0) | 0,0200% | 0,0400-0,0500%* |
| Regular + BNB discount | ~0,0180% | ~0,0450% |
| VIP1 | 0,0160% | 0,0400% |
| VIP9 | 0,0170% (meilleur tier) | — |

*Note : sources divergentes sur le taker de base (0,04% vs 0,05% selon la période/source
consultée) — **à vérifier sur ton compte réel avant backtest**, cette valeur a bougé
plusieurs fois en 2025-2026.

- Funding : toutes les 8h
- USDC-M perp : maker 0,00% / taker 0,04% (moins cher que USDT-M sur certaines paires)

---

## 2. Bybit

### Spot
Pas de données précises trouvées pour le tier de base — historiquement aligné sur
0,1%/0,1% comme la plupart des CEX. **À vérifier sur bybit.com/fee-rate avant usage.**

### USDT Perpetual (linear)
| Tier | Maker | Taker |
|---|---|---|
| Base (non-VIP) | 0,0200% | 0,0550% |

- Funding : toutes les 8h pour la majorité des paires ; certaines paires à 4h/2h/1h
- Levier max : jusqu'à 200x

---

## 3. OKX

### Spot
| Tier | Maker | Taker |
|---|---|---|
| Regular | 0,0800% | 0,1000% |

### USDT-M / USDC-M Perpetual & Futures (barème post-avril 2026)
| Tier | Groupe 1 (top 10 paires) Maker/Taker | Groupe 2 (autres) Maker/Taker |
|---|---|---|
| Regular | 0,0200% / 0,0500% | 0,0200% / 0,0500% |
| VIP1 | 0,0160% / 0,0450% | 0,0160% / 0,0450% |
| VIP2 | 0,0150% / 0,0360% | 0,0150% / 0,0360% |
| VIP3 | 0,0100% / 0,0280% | 0,0100% / 0,0280% |
| VIP4 | 0,0080% / 0,0270% | 0,0080% / 0,0270% |
| VIP5 | 0,0050% / 0,0260% | 0,0050% / 0,0260% |
| VIP6 | 0,0000% / 0,0250% | 0,0000% / 0,0250% |
| VIP7 | -0,0020% / 0,0200% | -0,0050% / 0,0250% |
| VIP8 | -0,0050% / 0,0200% | -0,0100% / 0,0250% |
| VIP9 | -0,0050% / 0,0150% | -0,0100% / 0,0200% |

Qualification VIP : le meilleur entre (actifs totaux du compte) OU (volume 30j sur
spot/perp/futures/options, évalués séparément).

- Funding : toutes les 8h
- Retrait limité à 10M$/24h en tier régulier

---

## 4. KuCoin

### Spot
| Classe de paire | Maker | Taker |
|---|---|---|
| Class A (VIP0, paires majeures) | 0,1000% | 0,1000% |
| Class B (VIP0) | 0,2000% | 0,2000% |
| Class C (VIP0) | 0,3000% | 0,3000% |

Discount possible via KCS (token natif) en plus des tiers VIP.

### Futures (perpetual + delivery)
| Tier | Maker | Taker |
|---|---|---|
| LV1 / VIP0 (base) | 0,0200% | 0,0600% |
| LV3 | 0,0100% | 0,0600% |
| LV4 | 0,0100% | 0,0530% → 0,0600%* |
| LV5 | 0,0060% | 0,0480% |
| LV8 | 0,0000% | 0,0360% |
| LV9-LV12 | négatif → **standardisé à 0% depuis mars 2026** (fin des rebates maker VIP) | 0,0250-0,0330% |

*Note : le taker LV4 a été relevé de 0,053% à 0,060% le 18 mai 2026 (annonce officielle) —
exemple concret de la volatilité de ces barèmes, à re-vérifier périodiquement.

- Funding : toutes les 8h
- Frais de liquidation additionnel sur delivery futures : 0,025% (pas de funding sur delivery)
- KYC obligatoire

---

## 5. Hyperliquid (DEX)

### Perpetuals
| Tier (volume roulant 14j) | Maker | Taker |
|---|---|---|
| Tier 0 (< 5M$) | 0,0150% | 0,0450% |
| Tier 1 (5M-25M$) | 0,0120% | 0,0400% |
| Tier 2 (25M-100M$) | 0,0080% | 0,0350% |
| Tier 3 (100M-500M$) | 0,0040% | 0,0300% |
| Tier 4 (500M-2B$) | 0,0010% | 0,0260% |
| Tier 5 (≥2B$) | 0,0000% (maker gratuit) | 0,0220% |

### Spot
Maker 0,0400% / Taker 0,0700%

- Funding : **toutes les heures** (vs 8h sur les CEX — impact non-négligeable sur un
  carry trade, à modéliser séparément)
- Fee calculée sur le notionnel complet, pas sur la marge (levier ne réduit pas la fee)
- Rebates maker supplémentaires possibles via staking HYPE
- Retrait : 1 USDC flat, pas de KYC

---

## 6. dYdX v4 (DEX)

### Perpetuals (seul marché — pas de spot)
| Tier | Volume 30j | Taker | Maker |
|---|---|---|---|
| 1 | < 1M$ | 5,0 bps | 1,0 bps |
| 2 | ≥ 1M$ | 4,5 bps | 1,0 bps |
| 3 | ≥ 5M$ | 4,0 bps | 0,5 bps |
| 4 | ≥ 25M$ | 3,5 bps | — |
| 5 | ≥ 125M$ | 3,0 bps | — |
| 6 | ≥125M$ + ≥0,5% part marché | 2,5 bps | -0,5 bps |
| 7 | ≥125M$ + ≥1% part maker | 2,5 bps | -0,7 bps |
| 8 | ≥125M$ + ≥2% part maker | 2,5 bps | -0,9 bps |
| 9 | ≥125M$ + ≥4% part maker | 2,5 bps | -1,1 bps |

- Funding : **toutes les heures**
- Volume calculé sur tous les sous-comptes et marchés cumulés
- Pas de KYC, frais de gas réseau pour les retraits
- ⚠️ N'est plus l'offre "free trading jusqu'à 100k$/mois" annoncée en 2022 — barème
  payant en vigueur depuis la migration v4

---

## Récapitulatif comparatif — tier de base, perpetuals

| Venue | Maker (bps) | Taker (bps) | Funding | KYC |
|---|---|---|---|---|
| Binance | 2,0 | 4,0-5,0* | 8h | Oui |
| Bybit | 2,0 | 5,5 | 8h (maj. paires) | Oui |
| OKX | 2,0 | 5,0 | 8h | Oui |
| KuCoin | 2,0 | 6,0 | 8h | Oui |
| Hyperliquid | 1,5 | 4,5 | **1h** | Non |
| dYdX v4 | 1,0 | 5,0 | **1h** | Non |

*à reconfirmer sur compte réel, valeur instable dans les sources trouvées.

**Points clés pour le backtesteur :**
1. Toutes les venues sont dans un mouchoir de poche côté taker (4,0-6,0 bps) — l'edge
   ne vient pas du choix de venue mais de l'exécution (maker vs taker) et du slippage réel.
2. dYdX et Hyperliquid ont un funding **8x plus fréquent** (1h vs 8h) — un carry trade
   comparé cross-venue doit annualiser correctement, pas juste comparer le taux affiché.
3. Les barèmes CEX changent souvent (ex. KuCoin LV4 taker relevé en cours d'année) —
   prévoir un paramètre de fee dans le backtesteur plutôt qu'une constante en dur, et
   idéalement dater chaque hypothèse de frais utilisée dans un run.
4. Aucune venue de la liste n'a de frais réellement nuls aujourd'hui, y compris dYdX.

## Limites de cette compilation
- Les barèmes VIP/tiers supérieurs évoluent vite et certains chiffres ci-dessus
  proviennent de sources tierces (blogs, agrégateurs) plutôt que de la doc officielle
  brute pour Bybit spot et KuCoin futures VIP1-2 — à recouper avec les pages fee-rate
  officielles avant un usage de production.
- Aucun frais de retrait, de financement de marge, ou de slippage n'est inclus ici :
  uniquement maker/taker + funding.
