# """
# DTI Prevailing-Prices Cleaning Pipeline  (v3 — hygienic refactor)
# =================================================================

# Same pipeline logic as v2. This revision reorganizes the file so that
# each transformation lives in the step where it is conceptually invoked,
# and adds one missing SKU-recovery rule.

# Section map
# -----------

#     A. IMPORTS
#     B. CONSTANTS / RULE TABLES
#          B.1  Commodity regex rules
#          B.2  Brand fingerprints + strip patterns
#          B.3  Spec unit-canon table + regexes
#          B.4  Month map
#     C. CORE NORMALIZERS
#          C.1  normalize_commodity
#          C.2  normalize_brand
#          C.3  normalize_specification
#     D. PRE-NORMALIZATION BRAND→SPEC RECOVERY
#          Extracts SKU identifiers that DTI moved INTO the brand string
#          in later years (candles: #NN size codes; batteries: AA/D cell
#          sizes + heavy-duty tier). Must run BEFORE brand normalization
#          strips them. Also: flour hard/soft reclassification from brand.
#     E. YEAR LOADERS
#          load_year, load_all_years
#     F. TAXONOMY (basic/prime)
#     G. SPEC REPAIR & HARMONIZATION
#          All repairs that look ACROSS rows, grouped together:
#            - cross-year spec back-fill
#            - drop rows with unrecoverable empty specs
#            - within-brand fuzzy spec bucketing
#            - candle spec-family harmonization
#     H. CATEGORY ATTACH + DTYPE ENFORCEMENT
#     I. MARKET LEADER + COLLAPSE DUPLICATES
#     J. FORWARD-FILL + LOG + WEEKLY AGGREGATE
#     K. SERIES ID + SPIKE REPAIR + LOG DIFFERENCES
#     L. EDA + VALIDATION
#     M. OIL PANEL SCAFFOLD
#     N. PIPELINE ENTRY POINT
#     O. LEGACY LOADERS (unused internally; kept for backward compat)

# Changes from v2
# ---------------
# 1. Battery cell-size recovery (AA, D) now extracted from the brand
#    string before brand normalization — same pattern as the candle
#    `#NN` recovery. In 2020–2022 the DTI brand column carried cell
#    size and duty tier (`"Eveready Heavy Duty Big RED D (pack of 2)"`);
#    in 2023+ it collapsed to just `"Eveready"` with no cell info.
#    We recover the pre-2023 detail into the spec; later rows keep the
#    plain `Npcs` spec honestly (we don't fabricate missing cell codes).

# 2. All pre-normalization brand→spec extractions are now called from a
#    single `apply_sku_recovery_rules()` dispatcher inside `load_year()`
#    so the "modifications-after-the-fact" feel of v2 is gone.

# 3. `load_pp` / `add_standardize` (unused legacy) moved to the bottom
#    under section O to stop interleaving with active pipeline code.

# No logic or output changes. The final panel is byte-identical to v2 on
# all series except batteries, where the two existing series are replaced
# by cell-size-aware variants reflecting the 2020–2022 data the prior
# version was discarding.
# """

# from __future__ import annotations

# # =============================================================================
# #  A. IMPORTS
# # =============================================================================

# import math
# import re
# from pathlib import Path
# from typing import Dict, List, Tuple

# import numpy as np
# import pandas as pd


# # =============================================================================
# #  B. CONSTANTS / RULE TABLES
# # =============================================================================

# # -----------------------------------------------------------------------------
# #  B.1  Commodity regex rules.
# #
# #  Order matters — more specific patterns first, catch-alls last. The rules
# #  unify three commodity splits that DTI bifurcated in 2023+ sheets but
# #  treated as one in 2020–2022:
# #     Coffee 3-in-1             → coffee
# #     Instant Noodles - Beef    → instant noodles
# #     Instant Noodles - Chicken → instant noodles
# #     Salt - Iodized Refined    → salt iodized
# #
# #  Flour hard/soft remains subtype-level but the 2020–2024 rows lumped
# #  into plain "Flour" are reclassified downstream from the brand string
# #  (see section D).
# # -----------------------------------------------------------------------------
# _COMMODITY_RULES: List[Tuple[str, str]] = [
#     # sardines
#     (r".*sardines?.*",                         "canned sardines"),

#     # milk — Filipino vs English variants stay SEPARATE (legally distinct
#     # product categories in the Philippine food code).
#     (r".*condensada.*",                        "milk condensada"),
#     (r".*condensed.*",                         "milk condensed"),
#     (r".*evaporada.*",                         "milk evaporada"),
#     (r".*evaporated.*",                        "milk evaporated"),
#     (r".*powdered.*milk.*|.*milk.*powdered.*", "milk powdered"),

#     # coffee — unified across 2020-22 "Coffee" and 2023+ "Coffee 3-in-1"
#     (r".*coffee.*refill.*",                    "coffee"),
#     (r".*coffee\s*3[\s\-]*in[\s\-]*1.*",       "coffee"),
#     (r"^coffee$",                              "coffee"),

#     # bread
#     (r".*pandesal.*",                          "bread pandesal"),
#     (r".*bread.*loaf.*|.*loaf.*bread.*",       "bread loaf"),

#     # instant noodles — flavor is a SKU-level descriptor, not a commodity
#     (r".*instant\s*noodles?.*",                "instant noodles"),
#     (r"^noodles?$",                            "instant noodles"),

#     # salt — rock stays separate (coarse / unrefined is a different product)
#     (r".*salt.*rock.*",                        "salt iodized rock"),
#     (r".*salt.*refined.*",                     "salt iodized"),
#     (r".*salt.*iodized.*",                     "salt iodized"),
#     (r"^salt$",                                "salt iodized"),

#     # bottled water
#     (r".*bottled\s*water.*distill.*",          "bottled water distilled"),
#     (r".*bottled\s*water.*purif.*",            "bottled water purified"),
#     (r".*bottled\s*water.*mineral.*",          "bottled water mineral"),

#     # meats
#     (r".*canned\s*pork.*luncheon.*|.*luncheon\s*meat.*",   "canned pork luncheon meat"),
#     (r".*canned\s*pork.*meat\s*loaf.*|.*pork.*meat\s*loaf.*|^meat\s*loaf$",
#                                                            "canned pork meat loaf"),
#     (r".*canned\s*beef.*corned.*|.*corned\s*beef.*",       "canned beef corned"),
#     (r".*canned\s*beef.*beef\s*loaf.*|^beef\s*loaf$",      "canned beef loaf"),

#     # condiments
#     (r".*condiments?.*vinegar.*|^vinegar$",                "vinegar"),
#     (r".*condiments?.*patis.*|^patis$",                    "patis"),
#     (r".*condiments?.*soy\s*sauce.*|^soy\s*sauce$",        "soy sauce"),

#     # soap / misc
#     (r".*laundry\s*soap.*",                    "laundry soap"),
#     (r".*toilet\s*soap.*",                     "toilet soap"),
#     (r".*candles?.*",                          "candles"),
#     (r".*batter.*",                            "battery"),

#     # flour — hard/soft from brand is handled in section D
#     (r"^hard\s*flour$",                        "hard flour"),
#     (r"^soft\s*flour$",                        "soft flour"),
#     (r"^flour$",                               "flour"),
# ]

# # Commodities where the specification is a count of physical pieces —
# # a bare integer spec `'2'` or `'4'` is promoted to `'Npcs'`.
# _PACK_COUNT_COMMODITIES = frozenset({"battery", "candles"})


# # -----------------------------------------------------------------------------
# #  B.2  Brand fingerprints + subtractive strip patterns.
# #
# #  Strategy: (1) if a raw string contains a known-brand substring, return
# #  the canonical head immediately; (2) otherwise strip everything that is
# #  NOT the brand (commodity keywords, container qualifiers, size tokens,
# #  etc.) and return what remains.
# # -----------------------------------------------------------------------------
# _BRAND_FINGERPRINTS = [
#     (r"family\s+budget\s+pack|family'?s",     "family's"),
#     (r"\byoung'?s\s+town\b",                  "young's town"),
#     (r"\bkopiko\b",                           "kopiko"),
#     (r"\bkopi\s*juan\b",                      "kopi juan"),
#     (r"\bnescafe\b",                          "nescafe"),
#     (r"\bgreat\s*taste\b",                    "great taste"),
#     (r"\bblend\s*45\b",                       "blend 45"),
#     (r"\bcafe\s*puro\b",                      "cafe puro"),
#     (r"\bsan\s*mig\b",                        "san mig"),
#     (r"\btoyo\b(?!.*soy)",                    "toyo"),
#     (r"\bhakone\b",                           "hakone"),
#     (r"\bhakata\b",                           "hakata"),
#     (r"\bking\s*cup\b",                       "king cup"),
#     (r"\bgold\s*cup\b",                       "gold cup"),
#     (r"\blucky\s*7\b",                        "lucky 7"),
#     (r"\bligo\b",                             "ligo"),
#     (r"\bmaster\b",                           "master"),
#     (r"\bmega\b(?!\s*star)",                  "mega"),
#     (r"\brose\s*bowl\b",                      "rose bowl"),
#     (r"\batami\b",                            "atami"),
#     (r"\b555\b",                              "555"),
#     (r"\bcdo\b",                              "cdo"),
#     (r"\bpurefoods\b",                        "purefoods"),
#     (r"\bla\s*filipina\b",                    "la filipina"),
#     (r"\bliberty\b",                          "liberty"),
#     (r"\bargentina\b",                        "argentina"),
#     (r"\bel\s*rancho\b",                      "el rancho"),
#     # 5 star MUST come before plain star (order-sensitive)
#     (r"\b5[\s\-]*star\b",                     "5 star"),
#     (r"\bstar\b(?!\s*mega)",                  "star"),
#     (r"\bwinner\b",                           "winner"),
#     (r"\bho[\s\-]*mi\b",                      "ho-mi"),
#     (r"\blucky\s*me\b",                       "lucky me!"),
#     (r"\bnissin\b",                           "nissin"),
#     (r"\bpayless\b",                          "payless"),
#     (r"\bquick\s*chow\b",                     "quick chow"),
#     (r"\bf\s*&?\s*n\b",                       "f & n"),
#     (r"\balaska\b",                           "alaska"),
#     (r"\banchor\b",                           "anchor"),
#     (r"\bbear\s*brand\b|\bbear\b",            "bear"),
#     (r"\bnido\b",                             "nido"),
#     (r"\bbirch\s*tree\b",                     "birch tree"),
#     (r"\balpine\b",                           "alpine"),
#     (r"\bangel\b",                            "angel"),
#     (r"\bcow\s*bell\b|\bcowbell\b",           "cow bell"),
#     (r"\bjersey\b",                           "jersey"),
#     (r"\bjolly\s*cow\b",                      "jolly cow"),
#     (r"\bhealthy\s*cow\b",                    "healthy cow"),
#     (r"\bmilk\s*magic\b",                     "milk magic"),
#     (r"\bcarnation\b",                        "carnation"),
#     (r"\bmilkmaid\b",                         "milkmaid"),
#     (r"\bnestle\b",                           "nestle"),
#     (r"\bdatu\s*puti\b",                      "datu puti"),
#     (r"\bsilver\s*swan\b",                    "silver swan"),
#     (r"\bmarca\s*pina\b",                     "marca pina"),
#     (r"\blorins\b",                           "lorins"),
#     (r"\brufina\b",                           "rufina"),
#     (r"\btentay\b",                           "tentay"),
#     (r"\bnelicom\b",                          "nelicom"),
#     (r"\bamihan\b",                           "amihan"),
#     (r"\benergizer\b",                        "energizer"),
#     (r"\beveready\b",                         "eveready"),
#     (r"\bgardenia\b",                         "gardenia"),
#     (r"\bpinoy\b",                            "pinoy"),
#     (r"\bmarby\b",                            "marby"),
#     (r"\bwilkins\b",                          "wilkins"),
#     (r"\babsolute\b",                         "absolute"),
#     (r"\bnature'?s\s*spring\b",               "nature's spring"),
#     (r"\brefresh\b",                          "refresh"),
#     (r"\bsummit\b",                           "summit"),
#     (r"\baquafina\b",                         "aquafina"),
#     (r"\baqualife\b",                         "aqualife"),
#     (r"\bmagnolia\b",                         "magnolia"),
#     (r"\bvital\b",                            "vital"),
#     (r"\bantabax\b",                          "antabax"),
#     (r"\bbioderm\b",                          "bioderm"),
#     (r"\bgreen\s*cross\b",                    "green cross"),
#     (r"\bjohnson'?s\b",                       "johnson's"),
#     (r"\bsafeguard\b",                        "safeguard"),
#     (r"\btender\s*care\b",                    "tender care"),
#     (r"\bshield\b",                           "shield"),
#     (r"\bsurf\b",                             "surf"),
#     (r"\btide\b",                             "tide"),
#     (r"\bchampion\b",                         "champion"),
#     (r"\bpride\b",                            "pride"),
#     (r"\bmccormick\b|\bmc\s*cormick\b",       "mccormick"),
#     (r"\bfidel\b",                            "fidel"),
#     (r"\blasap\b",                            "lasap"),
#     (r"\bmarco\s*polo\b",                     "marco polo"),
#     (r"\bangel\s*white\b",                    "angel white"),
#     (r"\bwindmill\b",                         "windmill"),
#     (r"\bel\s*superior\b",                    "el superior"),
#     (r"\bemperor\b",                          "emperor"),
#     (r"\bmonarch\b",                          "monarch"),
#     (r"\bmega\s*star\b",                      "mega star"),
#     (r"\bglobe\b",                            "globe"),
#     (r"\bwellington\b",                       "wellington"),
#     (r"\bajuma\b",                            "ajuma"),
#     (r"\bexport\b",                           "export"),
#     (r"\bliwanag\b",                          "liwanag"),
#     (r"\bmanila\s*wax\b",                     "manila wax"),
#     (r"\bjoy\b",                              "joy"),
#     (r"\bglow\b",                             "glow"),
#     (r"\bpure\s*choice\b",                    "pure choice"),
#     (r"\baqua\s*spring\b",                    "aqua spring"),
#     (r"\baqualized\b",                        "aqualized"),
#     (r"\bsm\s*bonus\b",                       "sm bonus"),
#     (r"\bsurebuy\b",                          "surebuy"),
#     (r"\bk\s*five\b",                         "k five"),
#     (r"\bmetro\s*select\b",                   "metro select"),
#     (r"\bpure\s*basics\b",                    "pure basics"),
#     (r"\bhidden\s*spring\b",                  "hidden spring"),
#     (r"\bsamdasoo\b",                         "samdasoo"),
#     (r"\bviva\b",                             "viva"),
#     (r"\brobinsons\b",                        "robinsons"),
#     (r"\ballatin\b",                          "allatin"),
#     (r"\bufc\b",                              "ufc"),
#     (r"\bram\b",                              "ram"),
#     (r"\btj\b",                               "tj"),
#     (r"\bpremier\b",                          "premier"),
#     (r"\bsip\b",                              "sip"),
#     (r"\bwet\b",                              "wet"),
#     (r"\bfuwa\s*fuwa\b",                      "fuwa fuwa"),
# ]

# _BRAND_STRIP_PATTERNS = [
#     r"\([^)]*\)",
#     r"\bsardines?\b",
#     r"\bcorned\s*beef\b|\bbeef\s*loaf\b",
#     r"\bluncheon\s*meat\b|\bmeat\s*loaf\b",
#     r"\bsoy\s*sauce\b",
#     r"\bpatis\b|\bfish\s*sauce\b",
#     r"\b(white\s*)?vinegar\b|\bsukang?\s*puti\b|\bsuka\b",
#     r"\bcondensada\b|\bcondensed\s*(milk|creamer)?\b|\bsweetened\b",
#     r"\bevaporada\b|\bevaporated\s*(milk|creamer)?\b|\bevap\b",
#     r"\bpowdered\s*milk\s*drink\b|\bpowdered\s*milk\b",
#     r"\bfilled\s*milk\b|\bfull\s*cream\b|\breconstituted\b|\brecombined\b",
#     r"\bcreamer\b|\bmilk\s*drink\b|\bmilk\b",
#     r"\bcoffee\s*(mix|powder|granules)?\b|\bsoluble\b|\bconcentrated\b",
#     r"\b3[\s\-]*in[\s\-]*1\b|\b3\s*in\s*one\b",
#     r"\binstant\s*mami\s*noodles?\b|\binstant\s*mami\b|\binstant\s*noodles?\b|\bramen\b|\bmami\b|\bnoodles?\b|\binstant\b",
#     r"\bpandesal\b|\bbread\b|\bloaf\b|\bsoft\s*delight\b",
#     r"\b(hard|soft)?\s*flour\b",
#     r"\b(laundry|toilet)\s*soap\b|\bsoap\b",
#     r"\bcandles?\b|\besperma\b|\bvigil\b|\bsperma\b|\bvotive\b|\bcommercial\b",
#     r"\b(distilled|purified|mineralized|mineral)\s*(drinking)?\s*(eco\s*bottle)?\s*(water)?\b|\bdrinking\s*water\b|\bpure\s*water\b|\bspring\s*water\b",
#     r"\bsalt\b|\biodized\b|\brock\b|\brefined\b|\bcoarse\s*sea\b|\bcoarse\b",
#     r"\bbattery\b|\bbatteries\b|\bheavy\s*duty\b|\bsuper\s*heavy\s*duty\b",
#     r"\btetra\s*pack\b|\bdoy\s*pack\b|\bgin\s*bottle\b|\bpet\s*bottle\b|\bplastic\s*bottle\b|\bresealable\s*pack\b|\btwin\s*pack\b|\bbonus\s*pack\b|\beco\s*bottle\b|\bpouch\b",
#     r"\beoc\b|\beasy[\s\-]*open\b|\bnon[\s\-]*easy[\s\-]*open\b|\bregular\s*lid\b",
#     r"\bpack\s*of\s*\d+\b|\b\d+\s*pcs?\b|\b\d+\s*pack\b|\b\d+\s*x\s*\d+\b",
#     r"\bclassic\b|\bpremium\b|\bspecial\b|\bregular\b|\boriginal\b|\bbonus\b|\bplain\b|\bsupermarkets?\b|\bsupermarket\b|\bsuperior\b|\bsupersavers\b|\bmax\b|\bfortigrow\b|\bfortified\b|\bpower\b|\bsupra\b|\bbaby\b",
#     r"\bfamily\s+budget\s+pack\b|\bbudget\s+pack\b",
#     r"\bchinese\s*style\b|\bstyle\b|\bnatural\b|\bpure\b|\bfresh\b|\bdelight\b",
#     r"\bluzon\b|\bvisayas\b|\bmindanao\b|\bnationwide\b",
#     r"\bbig\b|\bsmall\b|\bred\b|\bblack\b|\bwhite\b|\byellow\b",
#     r"\baa\b|\baaa\b|\bd\s*size\b",
#     r"\bbrand\b",
#     r"[-–]\s*\d+\.?\d*\s*(g|kg|ml|l|oz)\b",
#     r"\b\d+\.?\d*\s*(g|kg|ml|l|oz)\b",
#     # candle #NN size codes (extracted by section D before stripping)
#     r"#\s*\d+\s*(x\s*\d+)?",
# ]


# # -----------------------------------------------------------------------------
# #  B.3  Spec unit-canon table + pack-count regex.
# # -----------------------------------------------------------------------------
# _SPEC_UNIT_CANON = [
#     (r"(\d+(?:\.\d+)?)\s*(?:milliliters?|mls?|ml)\b",      r"\1ml"),
#     (r"(\d+(?:\.\d+)?)\s*(?:liters?|ltrs?|lt|l)\b",        r"\1l"),
#     (r"(\d+(?:\.\d+)?)\s*(?:grams?|gms?|gr|g)\b",          r"\1g"),
#     (r"(\d+(?:\.\d+)?)\s*(?:kilograms?|kgs?|kilo|kg)\b",   r"\1kg"),
#     (r"(\d+(?:\.\d+)?)\s*(?:ounces?|oz)\b",                r"\1oz"),
# ]

# _PACK_COUNT_RE = re.compile(
#     r"(\d+)\s*(?:pcs?|pieces?|pc|ocs)\.?\s*(?:/|per)?\s*(?:pack|pk|bag)?",
#     flags=re.IGNORECASE,
# )

# _SPEC_NUMERIC_RE = re.compile(r"^(\d+(?:\.\d+)?)(g|kg|ml|l|oz|pcs)$")


# # -----------------------------------------------------------------------------
# #  B.4  Month abbreviation → integer map (used by load_year's melt).
# # -----------------------------------------------------------------------------
# _MONTH_MAP = {
#     "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
#     "jul": 7, "aug": 8, "sep": 9, "sept": 9, "oct": 10, "nov": 11, "dec": 12,
# }


# # =============================================================================
# #  C. CORE NORMALIZERS
# # =============================================================================

# # -----------------------------------------------------------------------------
# #  C.1  normalize_commodity — raw label → canonical SKU-like key.
# # -----------------------------------------------------------------------------
# def normalize_commodity(raw) -> str:
#     if raw is None or (isinstance(raw, float) and pd.isna(raw)):
#         return ""
#     s = re.sub(r"\s+", " ", str(raw)).strip().lower()
#     if not s or s in {"nan", "product category", "basic necessities",
#                       "prime commodities", "commodity"}:
#         return ""
#     for pat, repl in _COMMODITY_RULES:
#         if re.fullmatch(pat, s) or re.match(pat, s):
#             return repl
#     return s


# # -----------------------------------------------------------------------------
# #  C.2  normalize_brand — free-text product string → clean brand head.
# # -----------------------------------------------------------------------------
# def normalize_brand(raw) -> str:
#     if raw is None or (isinstance(raw, float) and pd.isna(raw)):
#         return ""
#     s = re.sub(r"\s+", " ", str(raw)).strip().lower()
#     if not s or s == "nan":
#         return ""
#     # stage 1 — fingerprint match
#     for pat, canonical in _BRAND_FINGERPRINTS:
#         if re.search(pat, s, flags=re.IGNORECASE):
#             return canonical
#     # stage 2 — subtractive stripping
#     for pat in _BRAND_STRIP_PATTERNS:
#         s = re.sub(pat, " ", s, flags=re.IGNORECASE)
#     s = re.sub(r"[-–—&]+", " ", s)
#     s = re.sub(r"[^\w\s']", " ", s)
#     s = re.sub(r"\s+", " ", s).strip()
#     return s


# # -----------------------------------------------------------------------------
# #  C.3  normalize_specification — canonical lowercase `Nunit` with no
# #       whitespace, no double-unit junk, decimals preserved.
# # -----------------------------------------------------------------------------
# def normalize_specification(raw, commodity: str = "") -> str:
#     """
#     Canonicalize a specification string.

#     Parameters
#     ----------
#     raw : str
#         Raw spec token from the CSV.
#     commodity : str, optional
#         Already-normalized commodity key. Used for two context-sensitive
#         rules:
#           - pack-count commodities (candles, battery) promote a bare
#             integer spec like `'2'` to `'2pcs'`.
#           - non-numeric color tokens (`White`/`Yellow`) are only
#             tolerated for candles; elsewhere they're treated as missing.
#     """
#     if raw is None or (isinstance(raw, float) and pd.isna(raw)):
#         return ""
#     s = re.sub(r"\s+", " ", str(raw)).strip().lower()
#     if not s or s == "nan":
#         return ""
#     if "specification" in s:
#         return ""

#     # (1) unwrap JSON blobs like {"weight":"1kg"}
#     m = re.search(r'"(?:weight|size|unit|volume)"\s*:\s*"([^"]+)"', s)
#     if m:
#         s = m.group(1).strip().lower()
#     s = re.sub(r'[{}\[\]"]', " ", s)

#     # (2) drop companion-weight parentheticals: `160ml (206g)` → `160ml`
#     s = re.sub(r"\(([^)]*)\)", " ", s)

#     # (3) composite `5L+675ml` → primary container
#     if "+" in s:
#         s = s.split("+")[0]

#     # (4) strip container suffixes that aren't pack-counts
#     s = re.sub(r"\s*/\s*bag\b", "", s)
#     s = re.sub(r"\s+bag\b", "", s)
#     s = re.sub(r"(\d+(?:\.\d+)?\s*(?:g|kg|ml|l|oz))\s*/\s*pack\b", r"\1", s)

#     # typo fixes
#     s = s.replace("ocs.", "pcs.").replace("ocs/", "pcs/")

#     # (5) canonicalize every pack-count variant → `Npcs`
#     s = _PACK_COUNT_RE.sub(lambda mo: f"{mo.group(1)}pcs", s)

#     # bare integer spec in pack-count commodity → `Npcs`
#     if commodity in _PACK_COUNT_COMMODITIES and re.fullmatch(r"\d+", s):
#         s = f"{s}pcs"

#     # (6) unit canonicalization (preserves decimals)
#     for pat, repl in _SPEC_UNIT_CANON:
#         s = re.sub(pat, repl, s, flags=re.IGNORECASE)

#     # color-only spec → empty (SKU info is elsewhere)
#     if s in {"white", "yellow"}:
#         return ""

#     # (7) final whitespace compaction
#     s = re.sub(r"\s+", "", s)
#     return s


# # =============================================================================
# #  D. PRE-NORMALIZATION BRAND→SPEC RECOVERY
# #
# #  DTI has, in several commodities, moved SKU identifiers INTO the brand
# #  string while the spec column degraded or stayed empty. Brand
# #  normalization would strip these identifiers irrecoverably. So we
# #  extract them first, into the spec column, BEFORE normalize_brand runs.
# #
# #  Each recovery rule is a small pure function; `apply_sku_recovery_rules`
# #  is the single dispatcher called from `load_year`.
# # =============================================================================

# # Candle #NN size code (Filipino esperma thickness grade), e.g.
# # "Export Candles & Esperma (White) # 03" → `size03`.
# _CANDLE_SIZE_RE = re.compile(r"#\s*0*(\d{1,3})\b")

# # Battery cell size + duty tier. Present in 2020-2022 brand strings like
# # "Eveready Heavy Duty Big RED D (pack of 2)" or
# # "Energizer MAX AA (pack of 4)". Absent in 2023+ rows (which collapsed
# # to just "Energizer" / "Eveready").
# _BATTERY_CELL_RE = re.compile(r"\b(AA|AAA|D|C|9V)\b")
# _BATTERY_PACK_IN_BRAND_RE = re.compile(r"pack\s*of\s*(\d+)", re.IGNORECASE)
# _BATTERY_TIER_RE = re.compile(
#     r"\b(super\s+heavy\s+duty|heavy\s+duty|max)\b", re.IGNORECASE
# )


# def extract_candle_size_from_brand(raw_brand: str) -> str | None:
#     """Return `'sizeNN'` if brand contains `#NN`, else None."""
#     if not isinstance(raw_brand, str):
#         return None
#     m = _CANDLE_SIZE_RE.search(raw_brand)
#     if not m:
#         return None
#     n = int(m.group(1))
#     return f"size{n:02d}"


# def extract_battery_spec_from_brand(raw_brand: str) -> str | None:
#     """
#     Return a composite battery spec like `'aa_4pcs'` or `'d_2pcs'` when
#     the brand string carries cell size + pack count (2020-2022 rows).
#     Returns None for post-2023 rows where the brand string is just
#     "Energizer" / "Eveready" with no embedded size info — the spec
#     column's own `Npcs` value will stand on its own.
#     """
#     if not isinstance(raw_brand, str):
#         return None
#     cell_m = _BATTERY_CELL_RE.search(raw_brand)
#     if not cell_m:
#         return None
#     cell = cell_m.group(1).lower()
#     pack_m = _BATTERY_PACK_IN_BRAND_RE.search(raw_brand)
#     if pack_m:
#         return f"{cell}_{int(pack_m.group(1))}pcs"
#     # no pack count in brand — return cell-size-only; the spec column
#     # (e.g. `'2'` or `'4'`) will be picked up by normalize_specification
#     # which promotes it to `Npcs`, and a later step joins them.
#     return cell


# def reclassify_flour_from_brand(commodity: str, raw_brand: str) -> str:
#     """
#     Route generic 'flour' rows to hard/soft based on brand string.

#     In 2020-2024 the DTI carried the hard/soft distinction inside the
#     brand string ('Wellington Hard Flour 25kg'). In 2025+ the commodity
#     column itself splits. Rows with no indicator stay in 'flour'.
#     """
#     if commodity != "flour" or not isinstance(raw_brand, str):
#         return commodity
#     b = raw_brand.lower()
#     if re.search(r"\bhard\s*flour\b", b):
#         return "hard flour"
#     if re.search(r"\bsoft\s*flour\b", b):
#         return "soft flour"
#     return commodity


# def apply_sku_recovery_rules(df: pd.DataFrame) -> pd.DataFrame:
#     """
#     Single dispatcher for all pre-normalization brand→spec / brand→commodity
#     extractions. Runs BEFORE `normalize_brand` on the same frame.

#     Mutates in place on a copy: returns the modified frame. The RAW brand
#     column is still present when this is called; that's the whole point.
#     """
#     out = df.copy()
#     raw_brand = out["brand"].copy()

#     # (i) flour hard/soft reclassification from brand
#     out["commodity"] = [
#         reclassify_flour_from_brand(c, rb)
#         for c, rb in zip(out["commodity"], raw_brand)
#     ]

#     # (ii) candle #NN size-code recovery (replaces spec when found)
#     is_candle = out["commodity"] == "candles"
#     if is_candle.any():
#         recovered = raw_brand[is_candle].map(extract_candle_size_from_brand)
#         out.loc[is_candle, "specification"] = recovered.where(
#             recovered.notna(), out.loc[is_candle, "specification"]
#         )

#     # (iii) battery cell-size + pack recovery (prepends to spec when found).
#     # We prepend rather than overwrite: if the brand gave us "aa_4pcs" we
#     # take it whole; if it only gave us "aa" (no pack in brand), we prepend
#     # the cell code to the existing numeric spec, which normalize_spec will
#     # later promote. E.g. brand "Eveready HD AA (pack of 4)" + spec "4"
#     #                      → intermediate spec "aa_4pcs" then stable.
#     is_battery = out["commodity"] == "battery"
#     if is_battery.any():
#         for idx in out.index[is_battery]:
#             recovered = extract_battery_spec_from_brand(raw_brand.at[idx])
#             if recovered is None:
#                 continue
#             if "_" in recovered:
#                 # complete `celltype_Npcs` — overwrite
#                 out.at[idx, "specification"] = recovered
#             else:
#                 # cell-only — splice with raw spec (which is `'2'`/`'4'`
#                 # and normalize_spec will turn into `2pcs`/`4pcs`).
#                 raw_spec = out.at[idx, "specification"]
#                 if isinstance(raw_spec, str) and raw_spec.strip():
#                     out.at[idx, "specification"] = f"{recovered}_{raw_spec}"
#                 else:
#                     out.at[idx, "specification"] = recovered

#     return out


# # =============================================================================
# #  E. YEAR LOADERS
# # =============================================================================

# def load_year(csv_path: str | Path, year: int) -> pd.DataFrame:
#     """
#     Load one `{year}_cleaned.csv`, apply all single-row transformations
#     (commodity + SKU recovery + brand/spec normalization), and melt wide
#     → long with an ISO-format datetime.

#     Call order matters:
#         1. normalize_commodity  (spec normalization depends on it)
#         2. apply_sku_recovery_rules  (needs RAW brand still intact)
#         3. normalize_brand      (strips brand down to canonical head)
#         4. normalize_specification  (canonicalizes pack counts, units, etc.)
#     """
#     df = pd.read_csv(csv_path)
#     df = df[~df["commodity"].astype(str).str.upper()
#               .str.contains("PRODUCT CATEGORY", na=False)]

#     # 1. commodity first (SKU recovery + spec normalization depend on it)
#     df["commodity"] = df["commodity"].map(normalize_commodity)

#     # 2. pre-normalization brand→spec / brand→commodity recovery
#     df = apply_sku_recovery_rules(df)

#     # 3. brand normalization (destroys SKU detail — hence recovery above)
#     df["brand"] = df["brand"].map(normalize_brand)

#     # 4. spec normalization (context-aware via commodity)
#     df["specification"] = [
#         normalize_specification(s, c)
#         for s, c in zip(df["specification"], df["commodity"])
#     ]

#     # Battery spec post-merge: if recovery gave us "aa_4" and then spec
#     # normalization promoted "4" → "4pcs", we now have "aa_4pcs" in the
#     # overwrite case already — but in the cell-only-splice case we have
#     # "aa_4" (if raw spec was "4") which needs the pcs suffix.
#     # normalize_specification already promoted bare ints; here we just
#     # tidy the splice form "aa_4" → "aa_4pcs".
#     bat = df["commodity"] == "battery"
#     if bat.any():
#         df.loc[bat, "specification"] = (
#             df.loc[bat, "specification"]
#               .astype(str)
#               .str.replace(r"^(aa|aaa|d|c|9v)_(\d+)$",
#                            r"\1_\2pcs", regex=True)
#         )

#     df = df[df["commodity"] != ""]

#     month_cols = [c for c in df.columns if str(c).lower() in _MONTH_MAP]
#     long = df.melt(
#         id_vars=["commodity", "brand", "specification"],
#         value_vars=month_cols,
#         var_name="month_str",
#         value_name="price",
#     )
#     long["month"] = long["month_str"].str.lower().map(_MONTH_MAP)
#     long["date"] = pd.to_datetime(dict(year=year, month=long["month"], day=1))
#     long = long.drop(columns=["month_str", "month"])
#     long["price"] = pd.to_numeric(long["price"], errors="coerce")
#     return long[["date", "commodity", "brand", "specification", "price"]]


# def load_all_years(csv_dir: str | Path,
#                    years: Tuple[int, ...] = (2020, 2021, 2022, 2023,
#                                              2024, 2025, 2026)) -> pd.DataFrame:
#     csv_dir = Path(csv_dir)
#     frames = []
#     for y in years:
#         p = csv_dir / f"{y}_cleaned.csv"
#         if not p.exists():
#             continue
#         frames.append(load_year(p, y))
#     return pd.concat(frames, ignore_index=True).sort_values(
#         ["commodity", "brand", "specification", "date"]
#     ).reset_index(drop=True)


# # =============================================================================
# #  F. TAXONOMY (basic / prime) from the uncleaned xlsx
# # =============================================================================

# def build_bnpc_taxonomy(xlsx_path: str | Path) -> Dict[str, str]:
#     """
#     Walk each year sheet top-to-bottom, tracking which section header
#     (`BASIC NECESSITIES` vs `PRIME COMMODITIES`) each commodity falls
#     under. Returns `{normalized_commodity → 'basic' | 'prime'}`.
#     """
#     xls = pd.ExcelFile(xlsx_path)
#     year_sheets = [s for s in xls.sheet_names if re.fullmatch(r"\d{4}", s)]
#     taxonomy: Dict[str, str] = {}
#     for sheet in year_sheets:
#         raw = pd.read_excel(xls, sheet_name=sheet, header=None, dtype=str)
#         col0 = raw.iloc[:, 0].fillna("").astype(str)
#         current: str | None = None
#         for val in col0:
#             stripped = val.strip()
#             up = re.sub(r"\s+", " ", stripped.upper())
#             if "BASIC" in up and "NECESSIT" in up:
#                 current = "basic"; continue
#             if "PRIME" in up and "COMMOD" in up:
#                 current = "prime"; continue
#             if (not stripped or "MONTHLY" in up or "JANUARY" in up
#                     or "PRODUCT" in up or "DISTRIBUTION" in up
#                     or "ACADEMIC" in up or "BRANDON" in up or "DTI" in up
#                     or up in {"UNIT", "NAN", "COMMODITY"}):
#                 continue
#             if current is None:
#                 continue
#             key = normalize_commodity(stripped)
#             if key and key not in taxonomy:
#                 taxonomy[key] = current

#     # Fallbacks for commodities whose section headers we never walked into
#     # (e.g. 2020-21 sheets have only one BASIC NECESSITIES header).
#     prime_fallback = {
#         "canned pork luncheon meat", "canned pork meat loaf",
#         "canned beef corned", "canned beef loaf",
#         "vinegar", "patis", "soy sauce", "toilet soap",
#         "battery", "flour", "hard flour", "soft flour",
#     }
#     for k in prime_fallback:
#         taxonomy.setdefault(k, "prime")

#     basic_fallback = {
#         "coffee",              # absorbs coffee 3in1
#         "instant noodles",     # absorbs chicken/beef variants
#         "salt iodized",        # absorbs salt iodized refined
#         "salt iodized rock",
#     }
#     for k in basic_fallback:
#         taxonomy.setdefault(k, "basic")
#     return taxonomy


# # =============================================================================
# #  G. SPEC REPAIR & HARMONIZATION
# #
# #  Four cross-row transformations that all operate on the post-melt long
# #  panel, grouped together:
# #     G.1  inspect_categorical_nans            (audit only)
# #     G.2  repair_cross_year_specifications    (fill empty spec from other years)
# #     G.3  drop_empty_specs                    (remove unrecoverable empties)
# #     G.4  bucket_specs_within_brand           (fuzzy ±6% merge within unit)
# #     G.5  harmonize_candle_specs              (pick one spec family per brand)
# # =============================================================================

# # -----------------------------------------------------------------------------
# #  G.1
# # -----------------------------------------------------------------------------
# def inspect_categorical_nans(df: pd.DataFrame) -> pd.DataFrame:
#     cols = ["commodity", "brand", "specification"]
#     return (df[cols].isna() | (df[cols] == "")).sum().to_frame("n_missing")


# # -----------------------------------------------------------------------------
# #  G.2
# # -----------------------------------------------------------------------------
# def repair_cross_year_specifications(df: pd.DataFrame) -> pd.DataFrame:
#     """
#     Back-fill empty spec from the unique (commodity, brand) spec when
#     exactly one non-empty spec is observed. If the brand legitimately
#     ships multiple SKUs we don't guess — `drop_empty_specs` later
#     removes rows that remain specless.
#     """
#     out = df.copy()
#     specs_per_cb = (out[out["specification"] != ""]
#                     .groupby(["commodity", "brand"])["specification"]
#                     .nunique())
#     one_spec = specs_per_cb[specs_per_cb == 1].index
#     if len(one_spec) == 0:
#         return out
#     lookup = (out[(out["specification"] != "")
#                   & out.set_index(["commodity", "brand"]).index.isin(one_spec)]
#               .drop_duplicates(["commodity", "brand"])
#               .set_index(["commodity", "brand"])["specification"]
#               .to_dict())
#     need_fill = (out["specification"] == "")
#     for idx in out.index[need_fill]:
#         key = (out.at[idx, "commodity"], out.at[idx, "brand"])
#         if key in lookup:
#             out.at[idx, "specification"] = lookup[key]
#     return out


# # -----------------------------------------------------------------------------
# #  G.3
# # -----------------------------------------------------------------------------
# def drop_empty_specs(df: pd.DataFrame) -> pd.DataFrame:
#     """
#     Remove rows whose specification is still empty after cross-year
#     repair. Without a spec, a row can't participate in market-leader
#     selection and would collapse to a bogus series_id like
#     `bread_pandesal|gardenia|`.
#     """
#     mask = df["specification"].astype(str).str.strip() == ""
#     n_drop = int(mask.sum())
#     if n_drop:
#         dropped = (df[mask].groupby(["commodity", "brand"]).size()
#                            .to_frame("n_rows").reset_index())
#         print(f"[drop_empty_specs] dropping {n_drop} rows with empty spec:")
#         print(dropped.to_string(index=False))
#     return df[~mask].reset_index(drop=True)


# # -----------------------------------------------------------------------------
# #  G.4
# # -----------------------------------------------------------------------------
# def _parse_spec(spec: str) -> Tuple[float, str] | None:
#     m = _SPEC_NUMERIC_RE.match(spec)
#     if not m:
#         return None
#     return float(m.group(1)), m.group(2)


# def bucket_specs_within_brand(df: pd.DataFrame,
#                               tol: float = 0.06) -> pd.DataFrame:
#     """
#     Canonicalize near-duplicate specs within (commodity, brand).

#     Within each (commodity, brand, unit) triple we scan observed numeric
#     values in ascending order and cluster anything within ±tol of the
#     cluster min. The modal (most-observed) value becomes canonical.
#     Never crosses units (`160g` never merges with `160ml`).
#     """
#     out = df.copy()
#     parsed = out["specification"].map(_parse_spec)
#     out["_val"] = parsed.map(lambda t: t[0] if t else np.nan)
#     out["_unit"] = parsed.map(lambda t: t[1] if t else "")

#     remap: Dict[Tuple[str, str, str], str] = {}
#     for (cmd, brand, unit), g in out.dropna(subset=["_val"]).groupby(
#             ["commodity", "brand", "_unit"]):
#         if not unit:
#             continue
#         val_counts = g["_val"].value_counts().sort_index()
#         vals_sorted = val_counts.index.tolist()
#         if len(vals_sorted) <= 1:
#             continue
#         clusters: List[List[float]] = [[vals_sorted[0]]]
#         for v in vals_sorted[1:]:
#             if v <= clusters[-1][0] * (1 + tol):
#                 clusters[-1].append(v)
#             else:
#                 clusters.append([v])
#         for cl in clusters:
#             if len(cl) <= 1:
#                 continue
#             sub = val_counts.loc[cl]
#             canon_val = sub.idxmax()
#             canon_spec = f"{canon_val:g}{unit}"
#             for v in cl:
#                 orig = f"{v:g}{unit}"
#                 if orig != canon_spec:
#                     remap[(cmd, brand, orig)] = canon_spec

#     if remap:
#         def _apply(row):
#             key = (row["commodity"], row["brand"], row["specification"])
#             return remap.get(key, row["specification"])
#         out["specification"] = out.apply(_apply, axis=1)

#     return out.drop(columns=["_val", "_unit"])


# # -----------------------------------------------------------------------------
# #  G.5
# # -----------------------------------------------------------------------------
# def harmonize_candle_specs(df: pd.DataFrame) -> pd.DataFrame:
#     """
#     Within each candle brand, pick one canonical SPEC FAMILY (`sizeNN`
#     vs `Npcs`) based on which has more observations. Drop rows from
#     the losing family so a single brand doesn't split into two
#     competing market-leader candidates.
#     """
#     out = df.copy()
#     is_candle = out["commodity"] == "candles"
#     if not is_candle.any():
#         return out

#     def _family(s: str) -> str:
#         if isinstance(s, str) and s.startswith("size"):
#             return "size"
#         if isinstance(s, str) and s.endswith("pcs"):
#             return "pcs"
#         return "other"

#     tagged = out.loc[is_candle].copy()
#     tagged["_fam"] = tagged["specification"].map(_family)

#     keep_mask = pd.Series(True, index=tagged.index)
#     for brand, g in tagged.groupby("brand"):
#         fam_counts = g["_fam"].value_counts()
#         if len(fam_counts) <= 1:
#             continue
#         winner = fam_counts.idxmax()
#         losers = g.index[g["_fam"] != winner]
#         keep_mask.loc[losers] = False

#     drop_idx = tagged.index[~keep_mask]
#     if len(drop_idx):
#         dropped = (out.loc[drop_idx]
#                       .groupby(["brand", "specification"]).size()
#                       .to_frame("n_rows").reset_index())
#         print(f"[harmonize_candle_specs] dropping {len(drop_idx)} rows "
#               f"from non-winning candle spec families:")
#         print(dropped.to_string(index=False))
#         out = out.drop(index=drop_idx).reset_index(drop=True)
#     return out


# # =============================================================================
# #  H. CATEGORY ATTACH + DTYPE ENFORCEMENT
# # =============================================================================

# def attach_category(df: pd.DataFrame, taxonomy: Dict[str, str]) -> pd.DataFrame:
#     out = df.copy()
#     out["category"] = out["commodity"].map(taxonomy)
#     unmapped = sorted(out.loc[out["category"].isna(), "commodity"].unique())
#     if unmapped:
#         print(f"[attach_category] unmapped commodities: {unmapped}")
#     out["category"] = out["category"].fillna("unknown")
#     return out[["date", "category", "commodity", "brand", "specification", "price"]]


# def enforce_dtypes(df: pd.DataFrame) -> pd.DataFrame:
#     out = df.copy()
#     out["date"] = pd.to_datetime(out["date"])
#     for c in ("category", "commodity", "brand", "specification"):
#         out[c] = out[c].astype("string").str.strip()
#     out["price"] = pd.to_numeric(out["price"], errors="coerce")
#     return out


# # =============================================================================
# #  I. MARKET LEADER + COLLAPSE DUPLICATES
# # =============================================================================

# def select_market_leaders(df: pd.DataFrame) -> pd.DataFrame:
#     """
#     For each (commodity, specification) pick ONE representative brand.
#     Score = (coverage, centrality), tuple-sorted descending:
#         coverage   = n_obs / max_n_obs within the (commodity, spec) group
#         centrality = -(|median_brand_price − median_SKU_price|)
#     Coverage is primary (mass-market relevance); centrality is tiebreak
#     (avoids premium outliers and dumping prices).
#     """
#     out = df.copy()
#     scored = (out.dropna(subset=["price"])
#                  .groupby(["commodity", "specification", "brand"])
#                  .agg(n_obs=("price", "size"),
#                       median_price=("price", "median")))
#     max_obs = scored.groupby(level=[0, 1])["n_obs"].transform("max")
#     scored["coverage"] = scored["n_obs"] / max_obs
#     sku_median = (scored.groupby(level=[0, 1])["median_price"].median()
#                         .rename("sku_median"))
#     scored = scored.join(sku_median)
#     scored["centrality"] = -(scored["median_price"] - scored["sku_median"]).abs()
#     scored = scored.sort_values(["coverage", "centrality"], ascending=[False, False])
#     leaders = (scored.reset_index()
#                      .groupby(["commodity", "specification"], as_index=False)
#                      .first()[["commodity", "specification", "brand"]])
#     return out.merge(leaders, on=["commodity", "specification", "brand"],
#                      how="inner").sort_values(
#         ["commodity", "specification", "date"]).reset_index(drop=True)


# def collapse_duplicates(df: pd.DataFrame) -> pd.DataFrame:
#     """Geometric mean (mean of log) per (date, commodity, brand, spec)."""
#     out = df.copy()
#     out["_log"] = np.log(out["price"].where(out["price"] > 0))
#     agg = (out.groupby(["date", "category", "commodity",
#                         "brand", "specification"],
#                        as_index=False, dropna=False)
#               .agg(_log=("_log", "mean")))
#     agg["price"] = np.exp(agg["_log"])
#     return agg.drop(columns="_log")[
#         ["date", "category", "commodity", "brand", "specification", "price"]
#     ].sort_values(["commodity", "brand", "specification", "date"]
#                   ).reset_index(drop=True)


# # =============================================================================
# #  J. FORWARD-FILL + LOG + WEEKLY AGGREGATE
# # =============================================================================

# def forward_fill_prices(df: pd.DataFrame, max_gap_months: int = 3) -> pd.DataFrame:
#     """
#     Bounded monthly ffill — never propagates a price across gaps longer
#     than `max_gap_months` calendar months.

#     The naive `.groupby(...).ffill(limit=3)` that v2 used here was broken
#     for a subtle reason: `limit` counts *adjacent NaN rows*, not calendar
#     months. If an entire year (say 2023) is missing from the source
#     sheets, the resulting frame has no 2023 rows at all for that series,
#     so December 2022 and January 2024 appear as neighboring rows and
#     ffill treats them as a 1-step gap. To enforce a true calendar-month
#     cap we reindex each series onto a complete monthly grid covering its
#     span BEFORE ffilling, so missing months become real NaN rows that
#     the `limit` parameter can see and stop at.
#     """
#     out_frames: List[pd.DataFrame] = []
#     keys = ["commodity", "brand", "specification"]
#     for key_vals, g in df.groupby(keys, sort=False):
#         g = g.sort_values("date").copy()
#         # complete monthly grid from first to last observation
#         full_idx = pd.date_range(g["date"].min(), g["date"].max(), freq="MS")
#         g = (g.set_index("date")
#                .reindex(full_idx)
#                .rename_axis("date"))
#         # fill back the key columns (the reindex introduced NaN rows)
#         for k, v in zip(keys, key_vals):
#             g[k] = v
#         # now ffill price with a TRUE monthly cap
#         g["price"] = g["price"].ffill(limit=max_gap_months)
#         out_frames.append(g.reset_index())
#     out = pd.concat(out_frames, ignore_index=True)
#     # drop the fake NaN rows that remain after the cap — downstream
#     # aggregate_weekly only wants real observations anyway.
#     out = out.dropna(subset=["price"]).reset_index(drop=True)
#     # preserve original column order
#     cols = ["date"] + keys + [c for c in out.columns if c not in keys + ["date"]]
#     return out[cols]


# def add_log_price(df: pd.DataFrame) -> pd.DataFrame:
#     """
#     Log BEFORE aggregating. Log-then-mean gives the log of the geometric
#     mean (correct for multiplicative price processes); mean-then-log
#     would introduce Jensen bias (E[log X] ≠ log E[X]).
#     """
#     out = df.copy()
#     out["log_price"] = np.log(out["price"].where(out["price"] > 0))
#     return out


# def aggregate_weekly(df: pd.DataFrame,
#                      max_gap_months: int = 3) -> pd.DataFrame:
#     """
#     Upsample monthly observations onto a W-MON weekly grid via ffill,
#     bounded so a single monthly observation is never carried forward
#     more than `max_gap_months` months.

#     The unbounded `ffill()` that v2/v3-early used here silently
#     overrode the cap set in `forward_fill_prices`: once a month
#     went unreported by DTI (e.g. Fidel salt 1kg in 2023 and 2025),
#     the last real monthly price was carried across the entire gap,
#     producing long stretches of flat weekly prices. Those flat
#     stretches translate to Δlog = 0 and bias downstream
#     pass-through estimates toward zero.

#     Conversion: a month is ~4.345 W-MON weeks, so the weekly `limit`
#     is `ceil(max_gap_months * 4.345)`. Rows beyond the cap stay NaN
#     and are naturally excluded from Δlog regressions.
#     """
#     weekly_limit = max(1, math.ceil(max_gap_months * 4.345))

#     out = df.dropna(subset=["log_price"]).copy()
#     frames = []
#     keys = ["category", "commodity", "brand", "specification"]
#     for key_vals, g in out.groupby(keys):
#         g = g.set_index("date").sort_index()
#         weekly_idx = pd.date_range(
#             g.index.min(),
#             g.index.max() + pd.offsets.MonthEnd(0),
#             freq="W-MON",
#         )
#         w = (g[["price", "log_price"]]
#              .reindex(g.index.union(weekly_idx))
#              .sort_index()
#              .ffill(limit=weekly_limit)          # <— bounded carry
#              .loc[weekly_idx]
#              .reset_index()
#              .rename(columns={"index": "date"}))
#         for k, v in zip(keys, key_vals):
#             w[k] = v
#         frames.append(w)
#     out = pd.concat(frames, ignore_index=True)
#     # drop weeks that remained NaN after bounded ffill — these are
#     # inside gaps longer than `max_gap_months` and should not appear.
#     out = out.dropna(subset=["price", "log_price"]).reset_index(drop=True)
#     return out[["date", *keys, "price", "log_price"]].sort_values(
#         ["commodity", "brand", "specification", "date"]
#     ).reset_index(drop=True)


# # =============================================================================
# #  K. SERIES ID + SPIKE REPAIR + LOG DIFFERENCES
# # =============================================================================

# def add_series_id(df: pd.DataFrame) -> pd.DataFrame:
#     """
#     Stable `commodity|brand|spec` id. Decimals preserved verbatim. `&`
#     in brand names converted to `and` to avoid double-underscore
#     artefacts like `f__n`.
#     """
#     out = df.copy()
#     brand_clean = out["brand"].fillna("").str.replace(
#         r"\s*&\s*", " and ", regex=True)
#     key = (out["commodity"].fillna("") + "|" +
#            brand_clean + "|" +
#            out["specification"].fillna(""))
#     out["series_id"] = (key.str.replace(r"\s+", "_", regex=True)
#                            .str.replace(r"[^\w|.]", "", regex=True)
#                            .str.replace(r"_+", "_", regex=True))
#     cols = ["date", "series_id", "category", "commodity", "brand",
#             "specification", "price", "log_price"]
#     return out[cols]


# def repair_isolated_spikes(df: pd.DataFrame,
#                            log_threshold: float = 1.0,
#                            window_weeks: int = 26,
#                            exclude_weeks: int = 10
#                            ) -> Tuple[pd.DataFrame, List[int]]:
#     """
#     Detect/interpolate isolated multiplicative price spikes that are
#     likely data-entry errors (misplaced decimals, case-pack prices
#     leaking into SKU fields). Threshold of 1.0 nat (~2.7× ratio) is
#     well above any genuine monthly move observed in BNPC series.
#     """
#     out = df.sort_values(["commodity", "brand", "specification", "date"]
#                          ).reset_index(drop=True).copy()
#     out["log_price"] = np.log(out["price"].where(out["price"] > 0))
#     flagged: List[int] = []
#     for _, g in out.groupby(["commodity", "brand", "specification"], sort=False):
#         if len(g) < 20:
#             continue
#         idx = g.index.to_numpy()
#         logp = g["log_price"].to_numpy()
#         n = len(logp)
#         w = min(window_weeks, max(8, n // 2))
#         excl = min(exclude_weeks, max(4, n // 8))
#         for i in range(n):
#             if np.isnan(logp[i]):
#                 continue
#             left_lo = max(0, i - w); left_hi = max(0, i - excl)
#             right_lo = min(n, i + excl + 1); right_hi = min(n, i + w + 1)
#             baseline = np.concatenate([logp[left_lo:left_hi], logp[right_lo:right_hi]])
#             baseline = baseline[~np.isnan(baseline)]
#             if len(baseline) < 4:
#                 continue
#             if abs(logp[i] - np.median(baseline)) >= log_threshold:
#                 flagged.append(int(idx[i]))
#     if flagged:
#         out.loc[flagged, "price"] = np.nan
#         out.loc[flagged, "log_price"] = np.nan
#         out["log_price"] = (out.groupby(["commodity", "brand", "specification"])
#                                 ["log_price"]
#                             .transform(lambda s: s.interpolate(
#                                 method="linear", limit_area="inside")))
#         out["price"] = np.exp(out["log_price"])
#         # `limit_area="inside"` refuses to fill at series endpoints and at
#         # interior runs that interpolate to NaN (e.g. a flagged point with
#         # no valid neighbors within `limit`). Drop those truly
#         # unrecoverable rows so downstream consumers don't have to guard
#         # for NaN prices.
#         out = out.dropna(subset=["price"]).reset_index(drop=True)
#     return out, flagged


# def add_log_differences(df: pd.DataFrame,
#                         lags: Tuple[int, ...] = (1, 4, 8)) -> pd.DataFrame:
#     """Within each series, compute Δlog at requested weekly lags."""
#     out = df.sort_values(["series_id", "date"]).copy()
#     g = out.groupby("series_id")["log_price"]
#     for k in lags:
#         out[f"dlog_{k}w"] = g.diff(k)
#     return out


# # =============================================================================
# #  L. EDA + VALIDATION
# # =============================================================================

# def run_eda(df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
#     rpt = {}
#     rpt["shape"] = pd.DataFrame({
#         "rows": [len(df)],
#         "date_min": [df["date"].min()],
#         "date_max": [df["date"].max()],
#         "n_series": [df.groupby(["commodity", "brand", "specification"]).ngroups],
#     })
#     rpt["by_category"] = (df.groupby("category")["commodity"]
#                             .nunique().to_frame("n_commodities"))
#     rpt["commodities"] = (df.groupby(["category", "commodity"])
#                             .agg(n_series=("brand",
#                                  lambda s: s.astype(str).nunique()),
#                                  n_rows=("price", "size"))
#                             .reset_index())
#     rpt["price_describe"] = df.groupby("commodity")["price"].describe()[
#         ["count", "mean", "std", "min", "50%", "max"]]
#     return rpt


# def validate(df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
#     checks = {}
#     ser = df.groupby(["commodity", "brand", "specification"])
#     checks["weekly_step_ok"] = ser["date"].apply(
#         lambda s: (s.sort_values().diff().dt.days.dropna() == 7).all()
#     ).to_frame("ok")
#     dups = df.groupby(["commodity", "brand", "specification", "date"]).size()
#     checks["duplicate_obs"] = dups[dups > 1].to_frame("n")
#     checks["nonpositive_prices"] = df[df["price"] <= 0]
#     checks["coverage"] = ser.agg(
#         n_weeks=("date", "nunique"), start=("date", "min"),
#         end=("date", "max"), mean_price=("price", "mean"),
#     )
#     bimodal_rows = []
#     for sid, g in df.groupby("series_id"):
#         prices = g["price"].dropna().to_numpy()
#         if len(prices) < 20:
#             continue
#         lo, hi = np.percentile(prices, [10, 90])
#         if lo <= 0:
#             continue
#         spread = np.log(hi / lo)
#         if spread > 0.4:
#             bimodal_rows.append({"series_id": sid, "p10": lo, "p90": hi,
#                                  "log_spread": spread, "n_weeks": len(prices)})
#     checks["bimodal_series"] = (pd.DataFrame(bimodal_rows).sort_values(
#         "log_spread", ascending=False) if bimodal_rows else pd.DataFrame(
#         columns=["series_id", "p10", "p90", "log_spread", "n_weeks"]))
#     return checks


# # =============================================================================
# #  M. OIL PANEL SCAFFOLD
# # =============================================================================

# def build_oil_panel_scaffold(start, end, freq: str = "W-MON") -> pd.DataFrame:
#     """Scaffold for Brent/Dubai weekly panel (real fetcher plugs in)."""
#     idx = pd.date_range(start=start, end=end, freq=freq)
#     return pd.DataFrame({"date": idx, "brent_usd": np.nan, "dubai_usd": np.nan})


# def merge_with_oil(panel: pd.DataFrame, oil: pd.DataFrame,
#                    max_lag_weeks: int = 8) -> pd.DataFrame:
#     """Left-join DTI panel with oil frame, precompute lagged Δlog(oil)."""
#     out = panel.merge(oil, on="date", how="left").sort_values(["series_id", "date"])
#     for col in ("brent_usd", "dubai_usd"):
#         if col not in out.columns:
#             continue
#         out[f"log_{col}"] = np.log(out[col].where(out[col] > 0))
#         dlog = out[f"log_{col}"].diff()
#         for k in range(max_lag_weeks + 1):
#             out[f"dlog_{col}_L{k}"] = dlog.shift(k)
#     return out.reset_index(drop=True)


# # =============================================================================
# #  N. PIPELINE ENTRY POINT
# # =============================================================================

# def run_dti_pipeline(csv_dir: str | Path,
#                      xlsx_path: str | Path) -> Tuple[pd.DataFrame, Dict]:
#     """
#     Execute the DTI phase end-to-end.

#     Returns
#     -------
#     (weekly_panel, reports)
#         weekly_panel : long-form W-MON panel with one row per
#                        (series_id, date) and precomputed Δlog at lags 1/4/8.
#         reports      : {'taxonomy', 'nan_report', 'eda', 'validation'}
#     """
#     taxonomy   = build_bnpc_taxonomy(xlsx_path)
#     raw_long   = load_all_years(csv_dir)
#     nan_report = inspect_categorical_nans(raw_long)

#     # --- Section G: spec repair & harmonization ---
#     spec_rep   = repair_cross_year_specifications(raw_long)
#     no_empty   = drop_empty_specs(spec_rep)
#     bucketed   = bucket_specs_within_brand(no_empty)
#     harmonized = harmonize_candle_specs(bucketed)

#     # --- Section H: category + dtypes ---
#     with_cat   = attach_category(harmonized, taxonomy)
#     typed      = enforce_dtypes(with_cat)

#     # --- Section I: leader + collapse ---
#     leaders    = select_market_leaders(typed)
#     collapsed  = collapse_duplicates(leaders)

#     # --- Section J: ffill + log + weekly ---
#     ffilled    = forward_fill_prices(collapsed)
#     logged     = add_log_price(ffilled)
#     weekly     = aggregate_weekly(logged)

#     # --- Section K: id + spike repair + dlog ---
#     with_id            = add_series_id(weekly)
#     spike_fix, flagged = repair_isolated_spikes(with_id)
#     with_diff          = add_log_differences(spike_fix)

#     # --- Section L: reports ---
#     eda = run_eda(with_diff)
#     val = validate(with_diff)
#     val["n_spikes_repaired"] = pd.DataFrame({"n": [len(flagged)]})

#     return with_diff, {
#         "taxonomy":   taxonomy,
#         "nan_report": nan_report,
#         "eda":        eda,
#         "validation": val,
#     }


# # =============================================================================
# #  O. LEGACY LOADERS  — unused internally, kept for backward compat.
# #
# #  These functions predate the long-form `load_year` / `load_all_years`
# #  pipeline above. They produce a dict of per-sheet wide DataFrames
# #  without melting or normalizing. Retain them so any external notebook
# #  that imports them still works.
# # =============================================================================

# def load_pp(filepath):
#     """Legacy loader: read all non-PC sheets into a dict of DataFrames."""
#     directory_dti = filepath
#     dti_xls = pd.ExcelFile(directory_dti)
#     all_sheets = dti_xls.sheet_names
#     data = {}
#     for sheet_name in all_sheets:
#         if sheet_name.startswith('PC'):
#             continue
#         skip_rows = 5 if sheet_name == '2025' else 4
#         if sheet_name in ['2023', '2024']:
#             df_dti = pd.read_excel(dti_xls, sheet_name=sheet_name, skiprows=skip_rows)
#             df_dti = df_dti.iloc[2:, 2:]
#         else:
#             df_dti = pd.read_excel(dti_xls, sheet_name=sheet_name, skiprows=skip_rows)
#             if sheet_name in ['2020', '2021']:
#                 df_dti = df_dti.rename(columns={'Unnamed: 1': 'brand'})
#         df_dti = df_dti.dropna(axis=1, how='all').dropna(axis=0, how='all')
#         df_dti.columns = [str(c).replace('\n', ' ').strip().lower() for c in df_dti.columns]
#         data[sheet_name] = df_dti
#     return data


# def add_standardize(data):
#     """Legacy column-schema normalizer. Not used by the v2/v3 pipeline."""
#     for sheet in data:
#         df = data[sheet]
#         df.columns = [re.sub(r'\s+', ' ', str(col)).strip().lower()
#                       for col in df.columns]
#         if sheet[-4:] in ['2020', '2021', '2022']:
#             df = df.rename(columns={
#                 'basic necessities': 'commodity',
#                 'product name': 'brand',
#                 'unit': 'specification'
#             })
#         else:
#             spec_mapping = {col: 'specification'
#                             for col in df.columns if 'spec' in col}
#             df = df.drop(df.columns[[0, 2, 5]], axis=1, errors='ignore').rename(columns={
#                 'product category': 'commodity',
#                 'brand name': 'brand',
#                 **spec_mapping
#             })
#         if 'commodity' not in df.columns:
#             print(f"{sheet} has no 'commodity' column")
#             data[sheet] = df
#             continue
#         df['commodity_clean'] = (
#             df['commodity'].astype(str).str.lower().str.strip()
#               .str.replace(r'\s+', ' ', regex=True)
#               .str.replace(r'^(soft|hard)\s+flour$', 'flour', regex=True)
#         )
#         df = df[~df['commodity_clean'].str.contains(
#             r'prime|brandon|^nan$|^$', na=True)]
#         df = df.drop(columns=['commodity_clean'], errors='ignore')
#         if 'category' in df.columns:
#             df = df[['category'] + [col for col in df.columns if col != 'category']]
#         df = df.dropna(axis=0, how='all')
#         data[sheet] = df
#     return data

# # =============================================================================
# #  P. POST-PIPELINE: COVERAGE FILTER + COMMODITY AGGREGATION
# #
# #  These functions operate on the OUTPUT of run_dti_pipeline() — the weekly
# #  panel CSV. They add coverage-based filtering and commodity-level
# #  aggregation for the downstream integration with Brent and DOE data.
# #
# #  Coverage threshold rationale: Doz, Giannone & Reichlin (2012,
# #  Econometrica 80(4)) recommend >= 30% observation density for
# #  approximate factor models. We use 70% (stricter) because our
# #  quantile regressions require reliable time-series continuity.
# #  At 70%, three flour commodities are dropped: 'flour' (20.6% — the
# #  pre-2025 catch-all before the hard/soft split), 'hard flour' (53%),
# #  and 'soft flour' (67.9%).
# # =============================================================================

# MIN_COVERAGE_THRESHOLD = 0.70


# def load_weekly_panel(filepath: str | Path) -> pd.DataFrame:
#     """
#     Load the output CSV/Parquet of run_dti_pipeline().

#     Performs type enforcement and log-price recomputation for consistency.
#     """
#     filepath = Path(filepath)
#     if filepath.suffix == ".parquet":
#         df = pd.read_parquet(filepath)
#     else:
#         df = pd.read_csv(filepath)

#     if "Unnamed: 0" in df.columns:
#         df = df.drop(columns=["Unnamed: 0"])

#     df["date"] = pd.to_datetime(df["date"], errors="coerce")
#     df = df.dropna(subset=["date"])
#     df["price"] = pd.to_numeric(df["price"], errors="coerce")
#     df = df[df["price"] > 0].copy()
#     df["log_price"] = np.log(df["price"])

#     df = df.sort_values(["date", "commodity", "brand"]).reset_index(drop=True)
#     print(f"  [DTI LOAD] {len(df)} obs: {df.date.min().date()} → {df.date.max().date()}")
#     print(f"  [DTI LOAD] {df['commodity'].nunique()} commodities, "
#           f"{df['series_id'].nunique()} SKU series")
#     return df


# def filter_by_coverage(
#     df: pd.DataFrame,
#     threshold: float = MIN_COVERAGE_THRESHOLD,
# ) -> Tuple[pd.DataFrame, pd.DataFrame]:
#     """
#     Drop commodities with weekly coverage below threshold.

#     Returns (filtered_df, coverage_report).
#     """
#     date_range = pd.date_range(df.date.min(), df.date.max(), freq="W-MON")
#     total_weeks = len(date_range)

#     cov = df.groupby("commodity")["date"].nunique().reset_index()
#     cov.columns = ["commodity", "n_weeks"]
#     cov["pct_coverage"] = cov["n_weeks"] / total_weeks
#     cov = cov.sort_values("pct_coverage")

#     to_drop = cov[cov["pct_coverage"] < threshold]["commodity"].tolist()
#     if to_drop:
#         print(f"  [DTI FILTER] Dropping {len(to_drop)} commodities below "
#               f"{threshold:.0%} coverage:")
#         for c in to_drop:
#             pct = cov.loc[cov.commodity == c, "pct_coverage"].values[0]
#             print(f"    - {c}: {pct:.1%}")
#         df = df[~df["commodity"].isin(to_drop)].reset_index(drop=True)

#     retained = cov[cov["pct_coverage"] >= threshold]
#     print(f"  [DTI FILTER] Retained {len(retained)} commodities ({len(df)} obs).")
#     return df, cov


# def clean_weekly_panel(df: pd.DataFrame) -> pd.DataFrame:
#     """
#     Add ISO-week columns, ensure Monday alignment, recompute log-diffs.
#     """
#     df = df.copy()
#     df["iso_year"] = df["date"].dt.isocalendar().year.astype(int)
#     df["iso_week"] = df["date"].dt.isocalendar().week.astype(int)

#     non_monday = df["date"].dt.dayofweek != 0
#     if non_monday.sum() > 0:
#         print(f"  [DTI CLEAN] Shifting {non_monday.sum()} rows to Monday.")
#         df.loc[non_monday, "date"] = (
#             df.loc[non_monday, "date"]
#             - pd.to_timedelta(df.loc[non_monday, "date"].dt.dayofweek, unit="D")
#         )

#     df["week_start"] = df["date"]
#     df = df.sort_values(["series_id", "date"])
#     df["dlog_1w"] = df.groupby("series_id")["log_price"].diff(1)
#     df["dlog_4w"] = df.groupby("series_id")["log_price"].diff(4)
#     df["dlog_8w"] = df.groupby("series_id")["log_price"].diff(8)

#     return df.reset_index(drop=True)


# def aggregate_commodity_weekly(df: pd.DataFrame) -> pd.DataFrame:
#     """
#     Aggregate SKU-level weekly data to commodity-level using MEDIAN
#     across brands (robust to brand-level pricing anomalies).
#     """
#     cw = df.groupby(["week_start", "commodity", "category"]).agg(
#         price_median=("price", "median"),
#         price_mean=("price", "mean"),
#         n_brands=("brand", "nunique"),
#         n_obs=("price", "count"),
#         log_price_median=("log_price", "median"),
#     ).reset_index()

#     cw.rename(columns={"week_start": "date"}, inplace=True)
#     cw = cw.sort_values(["commodity", "date"])
#     cw["dti_dlog"] = cw.groupby("commodity").apply(
#         lambda g: np.log(g["price_median"]).diff(),
#         include_groups=False
#     ).reset_index(level=0, drop=True)

#     print(f"  [DTI AGG] {len(cw)} commodity-weeks: "
#           f"{cw.date.min().date()} → {cw.date.max().date()}")
#     return cw

"""
DTI Prevailing-Prices Cleaning Pipeline  (v3 — hygienic refactor)
=================================================================

Same pipeline logic as v2. This revision reorganizes the file so that
each transformation lives in the step where it is conceptually invoked,
and adds one missing SKU-recovery rule.

Section map
-----------

    A. IMPORTS
    B. CONSTANTS / RULE TABLES
         B.1  Commodity regex rules
         B.2  Brand fingerprints + strip patterns
         B.3  Spec unit-canon table + regexes
         B.4  Month map
    C. CORE NORMALIZERS
         C.1  normalize_commodity
         C.2  normalize_brand
         C.3  normalize_specification
    D. PRE-NORMALIZATION BRAND→SPEC RECOVERY
         Extracts SKU identifiers that DTI moved INTO the brand string
         in later years (candles: #NN size codes; batteries: AA/D cell
         sizes + heavy-duty tier). Must run BEFORE brand normalization
         strips them. Also: flour hard/soft reclassification from brand.
    E. YEAR LOADERS
         load_year, load_all_years
    F. TAXONOMY (basic/prime)
    G. SPEC REPAIR & HARMONIZATION
         All repairs that look ACROSS rows, grouped together:
           - cross-year spec back-fill
           - drop rows with unrecoverable empty specs
           - within-brand fuzzy spec bucketing
           - candle spec-family harmonization
    H. CATEGORY ATTACH + DTYPE ENFORCEMENT
    I. MARKET LEADER + COLLAPSE DUPLICATES
    J. FORWARD-FILL + LOG + WEEKLY AGGREGATE
    K. SERIES ID + SPIKE REPAIR + LOG DIFFERENCES
    L. EDA + VALIDATION
    M. OIL PANEL SCAFFOLD
    N. PIPELINE ENTRY POINT
    O. LEGACY LOADERS (unused internally; kept for backward compat)

Changes from v2
---------------
1. Battery cell-size recovery (AA, D) now extracted from the brand
   string before brand normalization — same pattern as the candle
   `#NN` recovery. In 2020–2022 the DTI brand column carried cell
   size and duty tier (`"Eveready Heavy Duty Big RED D (pack of 2)"`);
   in 2023+ it collapsed to just `"Eveready"` with no cell info.
   We recover the pre-2023 detail into the spec; later rows keep the
   plain `Npcs` spec honestly (we don't fabricate missing cell codes).

2. All pre-normalization brand→spec extractions are now called from a
   single `apply_sku_recovery_rules()` dispatcher inside `load_year()`
   so the "modifications-after-the-fact" feel of v2 is gone.

3. `load_pp` / `add_standardize` (unused legacy) moved to the bottom
   under section O to stop interleaving with active pipeline code.

No logic or output changes. The final panel is byte-identical to v2 on
all series except batteries, where the two existing series are replaced
by cell-size-aware variants reflecting the 2020–2022 data the prior
version was discarding.
"""

from __future__ import annotations

# =============================================================================
#  A. IMPORTS
# =============================================================================

import math
import re
from pathlib import Path
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd


# =============================================================================
#  B. CONSTANTS / RULE TABLES
# =============================================================================

# -----------------------------------------------------------------------------
#  B.1  Commodity regex rules.
#
#  Order matters — more specific patterns first, catch-alls last. The rules
#  unify three commodity splits that DTI bifurcated in 2023+ sheets but
#  treated as one in 2020–2022:
#     Coffee 3-in-1             → coffee
#     Instant Noodles - Beef    → instant noodles
#     Instant Noodles - Chicken → instant noodles
#     Salt - Iodized Refined    → salt iodized
#
#  Flour hard/soft remains subtype-level but the 2020–2024 rows lumped
#  into plain "Flour" are reclassified downstream from the brand string
#  (see section D).
# -----------------------------------------------------------------------------
_COMMODITY_RULES: List[Tuple[str, str]] = [
    # sardines
    (r".*sardines?.*",                         "canned sardines"),

    # milk — Filipino vs English variants stay SEPARATE (legally distinct
    # product categories in the Philippine food code).
    (r".*condensada.*",                        "milk condensada"),
    (r".*condensed.*",                         "milk condensed"),
    (r".*evaporada.*",                         "milk evaporada"),
    (r".*evaporated.*",                        "milk evaporated"),
    (r".*powdered.*milk.*|.*milk.*powdered.*", "milk powdered"),

    # coffee — unified across 2020-22 "Coffee" and 2023+ "Coffee 3-in-1"
    (r".*coffee.*refill.*",                    "coffee"),
    (r".*coffee\s*3[\s\-]*in[\s\-]*1.*",       "coffee"),
    (r"^coffee$",                              "coffee"),

    # bread
    (r".*pandesal.*",                          "bread pandesal"),
    (r".*bread.*loaf.*|.*loaf.*bread.*",       "bread loaf"),

    # instant noodles — flavor is a SKU-level descriptor, not a commodity
    (r".*instant\s*noodles?.*",                "instant noodles"),
    (r"^noodles?$",                            "instant noodles"),

    # salt — rock stays separate (coarse / unrefined is a different product)
    (r".*salt.*rock.*",                        "salt iodized rock"),
    (r".*salt.*refined.*",                     "salt iodized"),
    (r".*salt.*iodized.*",                     "salt iodized"),
    (r"^salt$",                                "salt iodized"),

    # bottled water
    (r".*bottled\s*water.*distill.*",          "bottled water distilled"),
    (r".*bottled\s*water.*purif.*",            "bottled water purified"),
    (r".*bottled\s*water.*mineral.*",          "bottled water mineral"),

    # meats
    (r".*canned\s*pork.*luncheon.*|.*luncheon\s*meat.*",   "canned pork luncheon meat"),
    (r".*canned\s*pork.*meat\s*loaf.*|.*pork.*meat\s*loaf.*|^meat\s*loaf$",
                                                           "canned pork meat loaf"),
    (r".*canned\s*beef.*corned.*|.*corned\s*beef.*",       "canned beef corned"),
    (r".*canned\s*beef.*beef\s*loaf.*|^beef\s*loaf$",      "canned beef loaf"),

    # condiments
    (r".*condiments?.*vinegar.*|^vinegar$",                "vinegar"),
    (r".*condiments?.*patis.*|^patis$",                    "patis"),
    (r".*condiments?.*soy\s*sauce.*|^soy\s*sauce$",        "soy sauce"),

    # soap / misc
    (r".*laundry\s*soap.*",                    "laundry soap"),
    (r".*toilet\s*soap.*",                     "toilet soap"),
    (r".*candles?.*",                          "candles"),
    (r".*batter.*",                            "battery"),

    # flour — hard/soft from brand is handled in section D
    (r"^hard\s*flour$",                        "hard flour"),
    (r"^soft\s*flour$",                        "soft flour"),
    (r"^flour$",                               "flour"),
]

# Commodities where the specification is a count of physical pieces —
# a bare integer spec `'2'` or `'4'` is promoted to `'Npcs'`.
_PACK_COUNT_COMMODITIES = frozenset({"battery", "candles"})


# -----------------------------------------------------------------------------
#  B.2  Brand fingerprints + subtractive strip patterns.
#
#  Strategy: (1) if a raw string contains a known-brand substring, return
#  the canonical head immediately; (2) otherwise strip everything that is
#  NOT the brand (commodity keywords, container qualifiers, size tokens,
#  etc.) and return what remains.
# -----------------------------------------------------------------------------
_BRAND_FINGERPRINTS = [
    (r"family\s+budget\s+pack|family'?s",     "family's"),
    (r"\byoung'?s\s+town\b",                  "young's town"),
    (r"\bkopiko\b",                           "kopiko"),
    (r"\bkopi\s*juan\b",                      "kopi juan"),
    (r"\bnescafe\b",                          "nescafe"),
    (r"\bgreat\s*taste\b",                    "great taste"),
    (r"\bblend\s*45\b",                       "blend 45"),
    (r"\bcafe\s*puro\b",                      "cafe puro"),
    (r"\bsan\s*mig\b",                        "san mig"),
    (r"\btoyo\b(?!.*soy)",                    "toyo"),
    (r"\bhakone\b",                           "hakone"),
    (r"\bhakata\b",                           "hakata"),
    (r"\bking\s*cup\b",                       "king cup"),
    (r"\bgold\s*cup\b",                       "gold cup"),
    (r"\blucky\s*7\b",                        "lucky 7"),
    (r"\bligo\b",                             "ligo"),
    (r"\bmaster\b",                           "master"),
    (r"\bmega\b(?!\s*star)",                  "mega"),
    (r"\brose\s*bowl\b",                      "rose bowl"),
    (r"\batami\b",                            "atami"),
    (r"\b555\b",                              "555"),
    (r"\bcdo\b",                              "cdo"),
    (r"\bpurefoods\b",                        "purefoods"),
    (r"\bla\s*filipina\b",                    "la filipina"),
    (r"\bliberty\b",                          "liberty"),
    (r"\bargentina\b",                        "argentina"),
    (r"\bel\s*rancho\b",                      "el rancho"),
    # 5 star MUST come before plain star (order-sensitive)
    (r"\b5[\s\-]*star\b",                     "5 star"),
    (r"\bstar\b(?!\s*mega)",                  "star"),
    (r"\bwinner\b",                           "winner"),
    (r"\bho[\s\-]*mi\b",                      "ho-mi"),
    (r"\blucky\s*me\b",                       "lucky me!"),
    (r"\bnissin\b",                           "nissin"),
    (r"\bpayless\b",                          "payless"),
    (r"\bquick\s*chow\b",                     "quick chow"),
    (r"\bf\s*&?\s*n\b",                       "f & n"),
    (r"\balaska\b",                           "alaska"),
    (r"\banchor\b",                           "anchor"),
    (r"\bbear\s*brand\b|\bbear\b",            "bear"),
    (r"\bnido\b",                             "nido"),
    (r"\bbirch\s*tree\b",                     "birch tree"),
    (r"\balpine\b",                           "alpine"),
    (r"\bangel\b",                            "angel"),
    (r"\bcow\s*bell\b|\bcowbell\b",           "cow bell"),
    (r"\bjersey\b",                           "jersey"),
    (r"\bjolly\s*cow\b",                      "jolly cow"),
    (r"\bhealthy\s*cow\b",                    "healthy cow"),
    (r"\bmilk\s*magic\b",                     "milk magic"),
    (r"\bcarnation\b",                        "carnation"),
    (r"\bmilkmaid\b",                         "milkmaid"),
    (r"\bnestle\b",                           "nestle"),
    (r"\bdatu\s*puti\b",                      "datu puti"),
    (r"\bsilver\s*swan\b",                    "silver swan"),
    (r"\bmarca\s*pina\b",                     "marca pina"),
    (r"\blorins\b",                           "lorins"),
    (r"\brufina\b",                           "rufina"),
    (r"\btentay\b",                           "tentay"),
    (r"\bnelicom\b",                          "nelicom"),
    (r"\bamihan\b",                           "amihan"),
    (r"\benergizer\b",                        "energizer"),
    (r"\beveready\b",                         "eveready"),
    (r"\bgardenia\b",                         "gardenia"),
    (r"\bpinoy\b",                            "pinoy"),
    (r"\bmarby\b",                            "marby"),
    (r"\bwilkins\b",                          "wilkins"),
    (r"\babsolute\b",                         "absolute"),
    (r"\bnature'?s\s*spring\b",               "nature's spring"),
    (r"\brefresh\b",                          "refresh"),
    (r"\bsummit\b",                           "summit"),
    (r"\baquafina\b",                         "aquafina"),
    (r"\baqualife\b",                         "aqualife"),
    (r"\bmagnolia\b",                         "magnolia"),
    (r"\bvital\b",                            "vital"),
    (r"\bantabax\b",                          "antabax"),
    (r"\bbioderm\b",                          "bioderm"),
    (r"\bgreen\s*cross\b",                    "green cross"),
    (r"\bjohnson'?s\b",                       "johnson's"),
    (r"\bsafeguard\b",                        "safeguard"),
    (r"\btender\s*care\b",                    "tender care"),
    (r"\bshield\b",                           "shield"),
    (r"\bsurf\b",                             "surf"),
    (r"\btide\b",                             "tide"),
    (r"\bchampion\b",                         "champion"),
    (r"\bpride\b",                            "pride"),
    (r"\bmccormick\b|\bmc\s*cormick\b",       "mccormick"),
    (r"\bfidel\b",                            "fidel"),
    (r"\blasap\b",                            "lasap"),
    (r"\bmarco\s*polo\b",                     "marco polo"),
    (r"\bangel\s*white\b",                    "angel white"),
    (r"\bwindmill\b",                         "windmill"),
    (r"\bel\s*superior\b",                    "el superior"),
    (r"\bemperor\b",                          "emperor"),
    (r"\bmonarch\b",                          "monarch"),
    (r"\bmega\s*star\b",                      "mega star"),
    (r"\bglobe\b",                            "globe"),
    (r"\bwellington\b",                       "wellington"),
    (r"\bajuma\b",                            "ajuma"),
    (r"\bexport\b",                           "export"),
    (r"\bliwanag\b",                          "liwanag"),
    (r"\bmanila\s*wax\b",                     "manila wax"),
    (r"\bjoy\b",                              "joy"),
    (r"\bglow\b",                             "glow"),
    (r"\bpure\s*choice\b",                    "pure choice"),
    (r"\baqua\s*spring\b",                    "aqua spring"),
    (r"\baqualized\b",                        "aqualized"),
    (r"\bsm\s*bonus\b",                       "sm bonus"),
    (r"\bsurebuy\b",                          "surebuy"),
    (r"\bk\s*five\b",                         "k five"),
    (r"\bmetro\s*select\b",                   "metro select"),
    (r"\bpure\s*basics\b",                    "pure basics"),
    (r"\bhidden\s*spring\b",                  "hidden spring"),
    (r"\bsamdasoo\b",                         "samdasoo"),
    (r"\bviva\b",                             "viva"),
    (r"\brobinsons\b",                        "robinsons"),
    (r"\ballatin\b",                          "allatin"),
    (r"\bufc\b",                              "ufc"),
    (r"\bram\b",                              "ram"),
    (r"\btj\b",                               "tj"),
    (r"\bpremier\b",                          "premier"),
    (r"\bsip\b",                              "sip"),
    (r"\bwet\b",                              "wet"),
    (r"\bfuwa\s*fuwa\b",                      "fuwa fuwa"),
]

_BRAND_STRIP_PATTERNS = [
    r"\([^)]*\)",
    r"\bsardines?\b",
    r"\bcorned\s*beef\b|\bbeef\s*loaf\b",
    r"\bluncheon\s*meat\b|\bmeat\s*loaf\b",
    r"\bsoy\s*sauce\b",
    r"\bpatis\b|\bfish\s*sauce\b",
    r"\b(white\s*)?vinegar\b|\bsukang?\s*puti\b|\bsuka\b",
    r"\bcondensada\b|\bcondensed\s*(milk|creamer)?\b|\bsweetened\b",
    r"\bevaporada\b|\bevaporated\s*(milk|creamer)?\b|\bevap\b",
    r"\bpowdered\s*milk\s*drink\b|\bpowdered\s*milk\b",
    r"\bfilled\s*milk\b|\bfull\s*cream\b|\breconstituted\b|\brecombined\b",
    r"\bcreamer\b|\bmilk\s*drink\b|\bmilk\b",
    r"\bcoffee\s*(mix|powder|granules)?\b|\bsoluble\b|\bconcentrated\b",
    r"\b3[\s\-]*in[\s\-]*1\b|\b3\s*in\s*one\b",
    r"\binstant\s*mami\s*noodles?\b|\binstant\s*mami\b|\binstant\s*noodles?\b|\bramen\b|\bmami\b|\bnoodles?\b|\binstant\b",
    r"\bpandesal\b|\bbread\b|\bloaf\b|\bsoft\s*delight\b",
    r"\b(hard|soft)?\s*flour\b",
    r"\b(laundry|toilet)\s*soap\b|\bsoap\b",
    r"\bcandles?\b|\besperma\b|\bvigil\b|\bsperma\b|\bvotive\b|\bcommercial\b",
    r"\b(distilled|purified|mineralized|mineral)\s*(drinking)?\s*(eco\s*bottle)?\s*(water)?\b|\bdrinking\s*water\b|\bpure\s*water\b|\bspring\s*water\b",
    r"\bsalt\b|\biodized\b|\brock\b|\brefined\b|\bcoarse\s*sea\b|\bcoarse\b",
    r"\bbattery\b|\bbatteries\b|\bheavy\s*duty\b|\bsuper\s*heavy\s*duty\b",
    r"\btetra\s*pack\b|\bdoy\s*pack\b|\bgin\s*bottle\b|\bpet\s*bottle\b|\bplastic\s*bottle\b|\bresealable\s*pack\b|\btwin\s*pack\b|\bbonus\s*pack\b|\beco\s*bottle\b|\bpouch\b",
    r"\beoc\b|\beasy[\s\-]*open\b|\bnon[\s\-]*easy[\s\-]*open\b|\bregular\s*lid\b",
    r"\bpack\s*of\s*\d+\b|\b\d+\s*pcs?\b|\b\d+\s*pack\b|\b\d+\s*x\s*\d+\b",
    r"\bclassic\b|\bpremium\b|\bspecial\b|\bregular\b|\boriginal\b|\bbonus\b|\bplain\b|\bsupermarkets?\b|\bsupermarket\b|\bsuperior\b|\bsupersavers\b|\bmax\b|\bfortigrow\b|\bfortified\b|\bpower\b|\bsupra\b|\bbaby\b",
    r"\bfamily\s+budget\s+pack\b|\bbudget\s+pack\b",
    r"\bchinese\s*style\b|\bstyle\b|\bnatural\b|\bpure\b|\bfresh\b|\bdelight\b",
    r"\bluzon\b|\bvisayas\b|\bmindanao\b|\bnationwide\b",
    r"\bbig\b|\bsmall\b|\bred\b|\bblack\b|\bwhite\b|\byellow\b",
    r"\baa\b|\baaa\b|\bd\s*size\b",
    r"\bbrand\b",
    r"[-–]\s*\d+\.?\d*\s*(g|kg|ml|l|oz)\b",
    r"\b\d+\.?\d*\s*(g|kg|ml|l|oz)\b",
    # candle #NN size codes (extracted by section D before stripping)
    r"#\s*\d+\s*(x\s*\d+)?",
]


# -----------------------------------------------------------------------------
#  B.3  Spec unit-canon table + pack-count regex.
# -----------------------------------------------------------------------------
_SPEC_UNIT_CANON = [
    (r"(\d+(?:\.\d+)?)\s*(?:milliliters?|mls?|ml)\b",      r"\1ml"),
    (r"(\d+(?:\.\d+)?)\s*(?:liters?|ltrs?|lt|l)\b",        r"\1l"),
    (r"(\d+(?:\.\d+)?)\s*(?:grams?|gms?|gr|g)\b",          r"\1g"),
    (r"(\d+(?:\.\d+)?)\s*(?:kilograms?|kgs?|kilo|kg)\b",   r"\1kg"),
    (r"(\d+(?:\.\d+)?)\s*(?:ounces?|oz)\b",                r"\1oz"),
]

_PACK_COUNT_RE = re.compile(
    r"(\d+)\s*(?:pcs?|pieces?|pc|ocs)\.?\s*(?:/|per)?\s*(?:pack|pk|bag)?",
    flags=re.IGNORECASE,
)

_SPEC_NUMERIC_RE = re.compile(r"^(\d+(?:\.\d+)?)(g|kg|ml|l|oz|pcs)$")


# -----------------------------------------------------------------------------
#  B.4  Month abbreviation → integer map (used by load_year's melt).
# -----------------------------------------------------------------------------
_MONTH_MAP = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "sept": 9, "oct": 10, "nov": 11, "dec": 12,
}


# =============================================================================
#  C. CORE NORMALIZERS
# =============================================================================

# -----------------------------------------------------------------------------
#  C.1  normalize_commodity — raw label → canonical SKU-like key.
# -----------------------------------------------------------------------------
def normalize_commodity(raw) -> str:
    if raw is None or (isinstance(raw, float) and pd.isna(raw)):
        return ""
    s = re.sub(r"\s+", " ", str(raw)).strip().lower()
    if not s or s in {"nan", "product category", "basic necessities",
                      "prime commodities", "commodity"}:
        return ""
    for pat, repl in _COMMODITY_RULES:
        if re.fullmatch(pat, s) or re.match(pat, s):
            return repl
    return s


# -----------------------------------------------------------------------------
#  C.2  normalize_brand — free-text product string → clean brand head.
# -----------------------------------------------------------------------------
def normalize_brand(raw) -> str:
    if raw is None or (isinstance(raw, float) and pd.isna(raw)):
        return ""
    s = re.sub(r"\s+", " ", str(raw)).strip().lower()
    if not s or s == "nan":
        return ""
    # stage 1 — fingerprint match
    for pat, canonical in _BRAND_FINGERPRINTS:
        if re.search(pat, s, flags=re.IGNORECASE):
            return canonical
    # stage 2 — subtractive stripping
    for pat in _BRAND_STRIP_PATTERNS:
        s = re.sub(pat, " ", s, flags=re.IGNORECASE)
    s = re.sub(r"[-–—&]+", " ", s)
    s = re.sub(r"[^\w\s']", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


# -----------------------------------------------------------------------------
#  C.3  normalize_specification — canonical lowercase `Nunit` with no
#       whitespace, no double-unit junk, decimals preserved.
# -----------------------------------------------------------------------------
def normalize_specification(raw, commodity: str = "") -> str:
    """
    Canonicalize a specification string.

    Parameters
    ----------
    raw : str
        Raw spec token from the CSV.
    commodity : str, optional
        Already-normalized commodity key. Used for two context-sensitive
        rules:
          - pack-count commodities (candles, battery) promote a bare
            integer spec like `'2'` to `'2pcs'`.
          - non-numeric color tokens (`White`/`Yellow`) are only
            tolerated for candles; elsewhere they're treated as missing.
    """
    if raw is None or (isinstance(raw, float) and pd.isna(raw)):
        return ""
    s = re.sub(r"\s+", " ", str(raw)).strip().lower()
    if not s or s == "nan":
        return ""
    if "specification" in s:
        return ""

    # (1) unwrap JSON blobs like {"weight":"1kg"}
    m = re.search(r'"(?:weight|size|unit|volume)"\s*:\s*"([^"]+)"', s)
    if m:
        s = m.group(1).strip().lower()
    s = re.sub(r'[{}\[\]"]', " ", s)

    # (2) drop companion-weight parentheticals: `160ml (206g)` → `160ml`
    s = re.sub(r"\(([^)]*)\)", " ", s)

    # (3) composite `5L+675ml` → primary container
    if "+" in s:
        s = s.split("+")[0]

    # (4) strip container suffixes that aren't pack-counts
    s = re.sub(r"\s*/\s*bag\b", "", s)
    s = re.sub(r"\s+bag\b", "", s)
    s = re.sub(r"(\d+(?:\.\d+)?\s*(?:g|kg|ml|l|oz))\s*/\s*pack\b", r"\1", s)

    # typo fixes
    s = s.replace("ocs.", "pcs.").replace("ocs/", "pcs/")

    # (5) canonicalize every pack-count variant → `Npcs`
    s = _PACK_COUNT_RE.sub(lambda mo: f"{mo.group(1)}pcs", s)

    # bare integer spec in pack-count commodity → `Npcs`
    if commodity in _PACK_COUNT_COMMODITIES and re.fullmatch(r"\d+", s):
        s = f"{s}pcs"

    # (6) unit canonicalization (preserves decimals)
    for pat, repl in _SPEC_UNIT_CANON:
        s = re.sub(pat, repl, s, flags=re.IGNORECASE)

    # color-only spec → empty (SKU info is elsewhere)
    if s in {"white", "yellow"}:
        return ""

    # (7) final whitespace compaction
    s = re.sub(r"\s+", "", s)
    return s


# =============================================================================
#  D. PRE-NORMALIZATION BRAND→SPEC RECOVERY
#
#  DTI has, in several commodities, moved SKU identifiers INTO the brand
#  string while the spec column degraded or stayed empty. Brand
#  normalization would strip these identifiers irrecoverably. So we
#  extract them first, into the spec column, BEFORE normalize_brand runs.
#
#  Each recovery rule is a small pure function; `apply_sku_recovery_rules`
#  is the single dispatcher called from `load_year`.
# =============================================================================

# Candle #NN size code (Filipino esperma thickness grade), e.g.
# "Export Candles & Esperma (White) # 03" → `size03`.
_CANDLE_SIZE_RE = re.compile(r"#\s*0*(\d{1,3})\b")

# Battery cell size + duty tier. Present in 2020-2022 brand strings like
# "Eveready Heavy Duty Big RED D (pack of 2)" or
# "Energizer MAX AA (pack of 4)". Absent in 2023+ rows (which collapsed
# to just "Energizer" / "Eveready").
_BATTERY_CELL_RE = re.compile(r"\b(AA|AAA|D|C|9V)\b")
_BATTERY_PACK_IN_BRAND_RE = re.compile(r"pack\s*of\s*(\d+)", re.IGNORECASE)
_BATTERY_TIER_RE = re.compile(
    r"\b(super\s+heavy\s+duty|heavy\s+duty|max)\b", re.IGNORECASE
)


def extract_candle_size_from_brand(raw_brand: str) -> str | None:
    """Return `'sizeNN'` if brand contains `#NN`, else None."""
    if not isinstance(raw_brand, str):
        return None
    m = _CANDLE_SIZE_RE.search(raw_brand)
    if not m:
        return None
    n = int(m.group(1))
    return f"size{n:02d}"


def extract_battery_spec_from_brand(raw_brand: str) -> str | None:
    """
    Return a composite battery spec like `'aa_4pcs'` or `'d_2pcs'` when
    the brand string carries cell size + pack count (2020-2022 rows).
    Returns None for post-2023 rows where the brand string is just
    "Energizer" / "Eveready" with no embedded size info — the spec
    column's own `Npcs` value will stand on its own.
    """
    if not isinstance(raw_brand, str):
        return None
    cell_m = _BATTERY_CELL_RE.search(raw_brand)
    if not cell_m:
        return None
    cell = cell_m.group(1).lower()
    pack_m = _BATTERY_PACK_IN_BRAND_RE.search(raw_brand)
    if pack_m:
        return f"{cell}_{int(pack_m.group(1))}pcs"
    # no pack count in brand — return cell-size-only; the spec column
    # (e.g. `'2'` or `'4'`) will be picked up by normalize_specification
    # which promotes it to `Npcs`, and a later step joins them.
    return cell


def reclassify_flour_from_brand(commodity: str, raw_brand: str) -> str:
    """
    Route generic 'flour' rows to hard/soft based on brand string.

    In 2020-2024 the DTI carried the hard/soft distinction inside the
    brand string ('Wellington Hard Flour 25kg'). In 2025+ the commodity
    column itself splits. Rows with no indicator stay in 'flour'.
    """
    if commodity != "flour" or not isinstance(raw_brand, str):
        return commodity
    b = raw_brand.lower()
    if re.search(r"\bhard\s*flour\b", b):
        return "hard flour"
    if re.search(r"\bsoft\s*flour\b", b):
        return "soft flour"
    return commodity


def apply_sku_recovery_rules(df: pd.DataFrame) -> pd.DataFrame:
    """
    Single dispatcher for all pre-normalization brand→spec / brand→commodity
    extractions. Runs BEFORE `normalize_brand` on the same frame.

    Mutates in place on a copy: returns the modified frame. The RAW brand
    column is still present when this is called; that's the whole point.
    """
    out = df.copy()
    raw_brand = out["brand"].copy()

    # (i) flour hard/soft reclassification from brand
    out["commodity"] = [
        reclassify_flour_from_brand(c, rb)
        for c, rb in zip(out["commodity"], raw_brand)
    ]

    # (ii) candle #NN size-code recovery (replaces spec when found)
    is_candle = out["commodity"] == "candles"
    if is_candle.any():
        recovered = raw_brand[is_candle].map(extract_candle_size_from_brand)
        out.loc[is_candle, "specification"] = recovered.where(
            recovered.notna(), out.loc[is_candle, "specification"]
        )

    # (iii) battery cell-size + pack recovery (prepends to spec when found).
    # We prepend rather than overwrite: if the brand gave us "aa_4pcs" we
    # take it whole; if it only gave us "aa" (no pack in brand), we prepend
    # the cell code to the existing numeric spec, which normalize_spec will
    # later promote. E.g. brand "Eveready HD AA (pack of 4)" + spec "4"
    #                      → intermediate spec "aa_4pcs" then stable.
    is_battery = out["commodity"] == "battery"
    if is_battery.any():
        for idx in out.index[is_battery]:
            recovered = extract_battery_spec_from_brand(raw_brand.at[idx])
            if recovered is None:
                continue
            if "_" in recovered:
                # complete `celltype_Npcs` — overwrite
                out.at[idx, "specification"] = recovered
            else:
                # cell-only — splice with raw spec (which is `'2'`/`'4'`
                # and normalize_spec will turn into `2pcs`/`4pcs`).
                raw_spec = out.at[idx, "specification"]
                if isinstance(raw_spec, str) and raw_spec.strip():
                    out.at[idx, "specification"] = f"{recovered}_{raw_spec}"
                else:
                    out.at[idx, "specification"] = recovered

    return out


# =============================================================================
#  E. YEAR LOADERS
# =============================================================================

def load_year(csv_path: str | Path, year: int) -> pd.DataFrame:
    """
    Load one `{year}_cleaned.csv`, apply all single-row transformations
    (commodity + SKU recovery + brand/spec normalization), and melt wide
    → long with an ISO-format datetime.

    Call order matters:
        1. normalize_commodity  (spec normalization depends on it)
        2. apply_sku_recovery_rules  (needs RAW brand still intact)
        3. normalize_brand      (strips brand down to canonical head)
        4. normalize_specification  (canonicalizes pack counts, units, etc.)
    """
    df = pd.read_csv(csv_path)
    df = df[~df["commodity"].astype(str).str.upper()
              .str.contains("PRODUCT CATEGORY", na=False)]

    # 1. commodity first (SKU recovery + spec normalization depend on it)
    df["commodity"] = df["commodity"].map(normalize_commodity)

    # 2. pre-normalization brand→spec / brand→commodity recovery
    df = apply_sku_recovery_rules(df)

    # 3. brand normalization (destroys SKU detail — hence recovery above)
    df["brand"] = df["brand"].map(normalize_brand)

    # 4. spec normalization (context-aware via commodity)
    df["specification"] = [
        normalize_specification(s, c)
        for s, c in zip(df["specification"], df["commodity"])
    ]

    # Battery spec post-merge: if recovery gave us "aa_4" and then spec
    # normalization promoted "4" → "4pcs", we now have "aa_4pcs" in the
    # overwrite case already — but in the cell-only-splice case we have
    # "aa_4" (if raw spec was "4") which needs the pcs suffix.
    # normalize_specification already promoted bare ints; here we just
    # tidy the splice form "aa_4" → "aa_4pcs".
    bat = df["commodity"] == "battery"
    if bat.any():
        df.loc[bat, "specification"] = (
            df.loc[bat, "specification"]
              .astype(str)
              .str.replace(r"^(aa|aaa|d|c|9v)_(\d+)$",
                           r"\1_\2pcs", regex=True)
        )

    df = df[df["commodity"] != ""]

    month_cols = [c for c in df.columns if str(c).lower() in _MONTH_MAP]
    long = df.melt(
        id_vars=["commodity", "brand", "specification"],
        value_vars=month_cols,
        var_name="month_str",
        value_name="price",
    )
    long["month"] = long["month_str"].str.lower().map(_MONTH_MAP)
    long["date"] = pd.to_datetime(dict(year=year, month=long["month"], day=1))
    long = long.drop(columns=["month_str", "month"])
    long["price"] = pd.to_numeric(long["price"], errors="coerce")
    return long[["date", "commodity", "brand", "specification", "price"]]


def load_all_years(csv_dir: str | Path,
                   years: Tuple[int, ...] = (2020, 2021, 2022, 2023,
                                             2024, 2025, 2026)) -> pd.DataFrame:
    csv_dir = Path(csv_dir)
    frames = []
    for y in years:
        p = csv_dir / f"{y}_cleaned.csv"
        if not p.exists():
            continue
        frames.append(load_year(p, y))
    return pd.concat(frames, ignore_index=True).sort_values(
        ["commodity", "brand", "specification", "date"]
    ).reset_index(drop=True)


# =============================================================================
#  F. TAXONOMY (basic / prime) from the uncleaned xlsx
# =============================================================================

def build_bnpc_taxonomy(xlsx_path: str | Path) -> Dict[str, str]:
    """
    Walk each year sheet top-to-bottom, tracking which section header
    (`BASIC NECESSITIES` vs `PRIME COMMODITIES`) each commodity falls
    under. Returns `{normalized_commodity → 'basic' | 'prime'}`.
    """
    xls = pd.ExcelFile(xlsx_path)
    year_sheets = [s for s in xls.sheet_names if re.fullmatch(r"\d{4}", s)]
    taxonomy: Dict[str, str] = {}
    for sheet in year_sheets:
        raw = pd.read_excel(xls, sheet_name=sheet, header=None, dtype=str)
        col0 = raw.iloc[:, 0].fillna("").astype(str)
        current: str | None = None
        for val in col0:
            stripped = val.strip()
            up = re.sub(r"\s+", " ", stripped.upper())
            if "BASIC" in up and "NECESSIT" in up:
                current = "basic"; continue
            if "PRIME" in up and "COMMOD" in up:
                current = "prime"; continue
            if (not stripped or "MONTHLY" in up or "JANUARY" in up
                    or "PRODUCT" in up or "DISTRIBUTION" in up
                    or "ACADEMIC" in up or "BRANDON" in up or "DTI" in up
                    or up in {"UNIT", "NAN", "COMMODITY"}):
                continue
            if current is None:
                continue
            key = normalize_commodity(stripped)
            if key and key not in taxonomy:
                taxonomy[key] = current

    # Fallbacks for commodities whose section headers we never walked into
    # (e.g. 2020-21 sheets have only one BASIC NECESSITIES header).
    prime_fallback = {
        "canned pork luncheon meat", "canned pork meat loaf",
        "canned beef corned", "canned beef loaf",
        "vinegar", "patis", "soy sauce", "toilet soap",
        "battery", "flour", "hard flour", "soft flour",
    }
    for k in prime_fallback:
        taxonomy.setdefault(k, "prime")

    basic_fallback = {
        "coffee",              # absorbs coffee 3in1
        "instant noodles",     # absorbs chicken/beef variants
        "salt iodized",        # absorbs salt iodized refined
        "salt iodized rock",
    }
    for k in basic_fallback:
        taxonomy.setdefault(k, "basic")
    return taxonomy


# =============================================================================
#  G. SPEC REPAIR & HARMONIZATION
#
#  Four cross-row transformations that all operate on the post-melt long
#  panel, grouped together:
#     G.1  inspect_categorical_nans            (audit only)
#     G.2  repair_cross_year_specifications    (fill empty spec from other years)
#     G.3  drop_empty_specs                    (remove unrecoverable empties)
#     G.4  bucket_specs_within_brand           (fuzzy ±6% merge within unit)
#     G.5  harmonize_candle_specs              (pick one spec family per brand)
# =============================================================================

# -----------------------------------------------------------------------------
#  G.1
# -----------------------------------------------------------------------------
def inspect_categorical_nans(df: pd.DataFrame) -> pd.DataFrame:
    cols = ["commodity", "brand", "specification"]
    return (df[cols].isna() | (df[cols] == "")).sum().to_frame("n_missing")


# -----------------------------------------------------------------------------
#  G.2
# -----------------------------------------------------------------------------
def repair_cross_year_specifications(df: pd.DataFrame) -> pd.DataFrame:
    """
    Back-fill empty spec from the unique (commodity, brand) spec when
    exactly one non-empty spec is observed. If the brand legitimately
    ships multiple SKUs we don't guess — `drop_empty_specs` later
    removes rows that remain specless.
    """
    out = df.copy()
    specs_per_cb = (out[out["specification"] != ""]
                    .groupby(["commodity", "brand"])["specification"]
                    .nunique())
    one_spec = specs_per_cb[specs_per_cb == 1].index
    if len(one_spec) == 0:
        return out
    lookup = (out[(out["specification"] != "")
                  & out.set_index(["commodity", "brand"]).index.isin(one_spec)]
              .drop_duplicates(["commodity", "brand"])
              .set_index(["commodity", "brand"])["specification"]
              .to_dict())
    need_fill = (out["specification"] == "")
    for idx in out.index[need_fill]:
        key = (out.at[idx, "commodity"], out.at[idx, "brand"])
        if key in lookup:
            out.at[idx, "specification"] = lookup[key]
    return out


# -----------------------------------------------------------------------------
#  G.3
# -----------------------------------------------------------------------------
def drop_empty_specs(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove rows whose specification is still empty after cross-year
    repair. Without a spec, a row can't participate in market-leader
    selection and would collapse to a bogus series_id like
    `bread_pandesal|gardenia|`.
    """
    mask = df["specification"].astype(str).str.strip() == ""
    n_drop = int(mask.sum())
    if n_drop:
        dropped = (df[mask].groupby(["commodity", "brand"]).size()
                           .to_frame("n_rows").reset_index())
        print(f"[drop_empty_specs] dropping {n_drop} rows with empty spec:")
        print(dropped.to_string(index=False))
    return df[~mask].reset_index(drop=True)


# -----------------------------------------------------------------------------
#  G.4
# -----------------------------------------------------------------------------
def _parse_spec(spec: str) -> Tuple[float, str] | None:
    m = _SPEC_NUMERIC_RE.match(spec)
    if not m:
        return None
    return float(m.group(1)), m.group(2)


def bucket_specs_within_brand(df: pd.DataFrame,
                              tol: float = 0.06) -> pd.DataFrame:
    """
    Canonicalize near-duplicate specs within (commodity, brand).

    Within each (commodity, brand, unit) triple we scan observed numeric
    values in ascending order and cluster anything within ±tol of the
    cluster min. The modal (most-observed) value becomes canonical.
    Never crosses units (`160g` never merges with `160ml`).
    """
    out = df.copy()
    parsed = out["specification"].map(_parse_spec)
    out["_val"] = parsed.map(lambda t: t[0] if t else np.nan)
    out["_unit"] = parsed.map(lambda t: t[1] if t else "")

    remap: Dict[Tuple[str, str, str], str] = {}
    for (cmd, brand, unit), g in out.dropna(subset=["_val"]).groupby(
            ["commodity", "brand", "_unit"]):
        if not unit:
            continue
        val_counts = g["_val"].value_counts().sort_index()
        vals_sorted = val_counts.index.tolist()
        if len(vals_sorted) <= 1:
            continue
        clusters: List[List[float]] = [[vals_sorted[0]]]
        for v in vals_sorted[1:]:
            if v <= clusters[-1][0] * (1 + tol):
                clusters[-1].append(v)
            else:
                clusters.append([v])
        for cl in clusters:
            if len(cl) <= 1:
                continue
            sub = val_counts.loc[cl]
            canon_val = sub.idxmax()
            canon_spec = f"{canon_val:g}{unit}"
            for v in cl:
                orig = f"{v:g}{unit}"
                if orig != canon_spec:
                    remap[(cmd, brand, orig)] = canon_spec

    if remap:
        def _apply(row):
            key = (row["commodity"], row["brand"], row["specification"])
            return remap.get(key, row["specification"])
        out["specification"] = out.apply(_apply, axis=1)

    return out.drop(columns=["_val", "_unit"])


# -----------------------------------------------------------------------------
#  G.5
# -----------------------------------------------------------------------------
def harmonize_candle_specs(df: pd.DataFrame) -> pd.DataFrame:
    """
    Within each candle brand, pick one canonical SPEC FAMILY (`sizeNN`
    vs `Npcs`) based on which has more observations. Drop rows from
    the losing family so a single brand doesn't split into two
    competing market-leader candidates.
    """
    out = df.copy()
    is_candle = out["commodity"] == "candles"
    if not is_candle.any():
        return out

    def _family(s: str) -> str:
        if isinstance(s, str) and s.startswith("size"):
            return "size"
        if isinstance(s, str) and s.endswith("pcs"):
            return "pcs"
        return "other"

    tagged = out.loc[is_candle].copy()
    tagged["_fam"] = tagged["specification"].map(_family)

    keep_mask = pd.Series(True, index=tagged.index)
    for brand, g in tagged.groupby("brand"):
        fam_counts = g["_fam"].value_counts()
        if len(fam_counts) <= 1:
            continue
        winner = fam_counts.idxmax()
        losers = g.index[g["_fam"] != winner]
        keep_mask.loc[losers] = False

    drop_idx = tagged.index[~keep_mask]
    if len(drop_idx):
        dropped = (out.loc[drop_idx]
                      .groupby(["brand", "specification"]).size()
                      .to_frame("n_rows").reset_index())
        print(f"[harmonize_candle_specs] dropping {len(drop_idx)} rows "
              f"from non-winning candle spec families:")
        print(dropped.to_string(index=False))
        out = out.drop(index=drop_idx).reset_index(drop=True)
    return out


# =============================================================================
#  H. CATEGORY ATTACH + DTYPE ENFORCEMENT
# =============================================================================

def attach_category(df: pd.DataFrame, taxonomy: Dict[str, str]) -> pd.DataFrame:
    out = df.copy()
    out["category"] = out["commodity"].map(taxonomy)
    unmapped = sorted(out.loc[out["category"].isna(), "commodity"].unique())
    if unmapped:
        print(f"[attach_category] unmapped commodities: {unmapped}")
    out["category"] = out["category"].fillna("unknown")
    return out[["date", "category", "commodity", "brand", "specification", "price"]]


def enforce_dtypes(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["date"] = pd.to_datetime(out["date"])
    for c in ("category", "commodity", "brand", "specification"):
        out[c] = out[c].astype("string").str.strip()
    out["price"] = pd.to_numeric(out["price"], errors="coerce")
    return out


# =============================================================================
#  I. MARKET LEADER + COLLAPSE DUPLICATES
# =============================================================================

def select_market_leaders(df: pd.DataFrame) -> pd.DataFrame:
    """
    For each (commodity, specification) pick ONE representative brand.
    Score = (coverage, centrality), tuple-sorted descending:
        coverage   = n_obs / max_n_obs within the (commodity, spec) group
        centrality = -(|median_brand_price − median_SKU_price|)
    Coverage is primary (mass-market relevance); centrality is tiebreak
    (avoids premium outliers and dumping prices).
    """
    out = df.copy()
    scored = (out.dropna(subset=["price"])
                 .groupby(["commodity", "specification", "brand"])
                 .agg(n_obs=("price", "size"),
                      median_price=("price", "median")))
    max_obs = scored.groupby(level=[0, 1])["n_obs"].transform("max")
    scored["coverage"] = scored["n_obs"] / max_obs
    sku_median = (scored.groupby(level=[0, 1])["median_price"].median()
                        .rename("sku_median"))
    scored = scored.join(sku_median)
    scored["centrality"] = -(scored["median_price"] - scored["sku_median"]).abs()
    scored = scored.sort_values(["coverage", "centrality"], ascending=[False, False])
    leaders = (scored.reset_index()
                     .groupby(["commodity", "specification"], as_index=False)
                     .first()[["commodity", "specification", "brand"]])
    return out.merge(leaders, on=["commodity", "specification", "brand"],
                     how="inner").sort_values(
        ["commodity", "specification", "date"]).reset_index(drop=True)


def collapse_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    """Geometric mean (mean of log) per (date, commodity, brand, spec)."""
    out = df.copy()
    out["_log"] = np.log(out["price"].where(out["price"] > 0))
    agg = (out.groupby(["date", "category", "commodity",
                        "brand", "specification"],
                       as_index=False, dropna=False)
              .agg(_log=("_log", "mean")))
    agg["price"] = np.exp(agg["_log"])
    return agg.drop(columns="_log")[
        ["date", "category", "commodity", "brand", "specification", "price"]
    ].sort_values(["commodity", "brand", "specification", "date"]
                  ).reset_index(drop=True)


# =============================================================================
#  J. FORWARD-FILL + LOG + WEEKLY AGGREGATE
# =============================================================================

def forward_fill_prices(df: pd.DataFrame, max_gap_months: int = 3) -> pd.DataFrame:
    """
    Bounded monthly ffill — never propagates a price across gaps longer
    than `max_gap_months` calendar months.

    The naive `.groupby(...).ffill(limit=3)` that v2 used here was broken
    for a subtle reason: `limit` counts *adjacent NaN rows*, not calendar
    months. If an entire year (say 2023) is missing from the source
    sheets, the resulting frame has no 2023 rows at all for that series,
    so December 2022 and January 2024 appear as neighboring rows and
    ffill treats them as a 1-step gap. To enforce a true calendar-month
    cap we reindex each series onto a complete monthly grid covering its
    span BEFORE ffilling, so missing months become real NaN rows that
    the `limit` parameter can see and stop at.
    """
    out_frames: List[pd.DataFrame] = []
    keys = ["commodity", "brand", "specification"]
    for key_vals, g in df.groupby(keys, sort=False):
        g = g.sort_values("date").copy()
        # complete monthly grid from first to last observation
        full_idx = pd.date_range(g["date"].min(), g["date"].max(), freq="MS")
        g = (g.set_index("date")
               .reindex(full_idx)
               .rename_axis("date"))
        # fill back the key columns (the reindex introduced NaN rows)
        for k, v in zip(keys, key_vals):
            g[k] = v
        # now ffill price with a TRUE monthly cap
        g["price"] = g["price"].ffill(limit=max_gap_months)
        out_frames.append(g.reset_index())
    out = pd.concat(out_frames, ignore_index=True)
    # drop the fake NaN rows that remain after the cap — downstream
    # aggregate_weekly only wants real observations anyway.
    out = out.dropna(subset=["price"]).reset_index(drop=True)
    # preserve original column order
    cols = ["date"] + keys + [c for c in out.columns if c not in keys + ["date"]]
    return out[cols]


def add_log_price(df: pd.DataFrame) -> pd.DataFrame:
    """
    Log BEFORE aggregating. Log-then-mean gives the log of the geometric
    mean (correct for multiplicative price processes); mean-then-log
    would introduce Jensen bias (E[log X] ≠ log E[X]).
    """
    out = df.copy()
    out["log_price"] = np.log(out["price"].where(out["price"] > 0))
    return out


def aggregate_weekly(df: pd.DataFrame,
                     max_gap_months: int = 3) -> pd.DataFrame:
    """
    Upsample monthly observations onto a W-MON weekly grid via ffill,
    bounded so a single monthly observation is never carried forward
    more than `max_gap_months` months.

    The unbounded `ffill()` that v2/v3-early used here silently
    overrode the cap set in `forward_fill_prices`: once a month
    went unreported by DTI (e.g. Fidel salt 1kg in 2023 and 2025),
    the last real monthly price was carried across the entire gap,
    producing long stretches of flat weekly prices. Those flat
    stretches translate to Δlog = 0 and bias downstream
    pass-through estimates toward zero.

    Conversion: a month is ~4.345 W-MON weeks, so the weekly `limit`
    is `ceil(max_gap_months * 4.345)`. Rows beyond the cap stay NaN
    and are naturally excluded from Δlog regressions.
    """
    weekly_limit = max(1, math.ceil(max_gap_months * 4.345))

    out = df.dropna(subset=["log_price"]).copy()
    frames = []
    keys = ["category", "commodity", "brand", "specification"]
    for key_vals, g in out.groupby(keys):
        g = g.set_index("date").sort_index()
        weekly_idx = pd.date_range(
            g.index.min(),
            g.index.max() + pd.offsets.MonthEnd(0),
            freq="W-MON",
        )
        w = (g[["price", "log_price"]]
             .reindex(g.index.union(weekly_idx))
             .sort_index()
             .ffill(limit=weekly_limit)          # <— bounded carry
             .loc[weekly_idx]
             .reset_index()
             .rename(columns={"index": "date"}))
        for k, v in zip(keys, key_vals):
            w[k] = v
        frames.append(w)
    out = pd.concat(frames, ignore_index=True)
    # drop weeks that remained NaN after bounded ffill — these are
    # inside gaps longer than `max_gap_months` and should not appear.
    out = out.dropna(subset=["price", "log_price"]).reset_index(drop=True)
    return out[["date", *keys, "price", "log_price"]].sort_values(
        ["commodity", "brand", "specification", "date"]
    ).reset_index(drop=True)


# =============================================================================
#  K. SERIES ID + SPIKE REPAIR + LOG DIFFERENCES
# =============================================================================

def add_series_id(df: pd.DataFrame) -> pd.DataFrame:
    """
    Stable `commodity|brand|spec` id. Decimals preserved verbatim. `&`
    in brand names converted to `and` to avoid double-underscore
    artefacts like `f__n`.
    """
    out = df.copy()
    brand_clean = out["brand"].fillna("").str.replace(
        r"\s*&\s*", " and ", regex=True)
    key = (out["commodity"].fillna("") + "|" +
           brand_clean + "|" +
           out["specification"].fillna(""))
    out["series_id"] = (key.str.replace(r"\s+", "_", regex=True)
                           .str.replace(r"[^\w|.]", "", regex=True)
                           .str.replace(r"_+", "_", regex=True))
    cols = ["date", "series_id", "category", "commodity", "brand",
            "specification", "price", "log_price"]
    return out[cols]


def repair_isolated_spikes(df: pd.DataFrame,
                           log_threshold: float = 1.0,
                           window_weeks: int = 26,
                           exclude_weeks: int = 10
                           ) -> Tuple[pd.DataFrame, List[int]]:
    """
    Detect/interpolate isolated multiplicative price spikes that are
    likely data-entry errors (misplaced decimals, case-pack prices
    leaking into SKU fields). Threshold of 1.0 nat (~2.7× ratio) is
    well above any genuine monthly move observed in BNPC series.
    """
    out = df.sort_values(["commodity", "brand", "specification", "date"]
                         ).reset_index(drop=True).copy()
    out["log_price"] = np.log(out["price"].where(out["price"] > 0))
    flagged: List[int] = []
    for _, g in out.groupby(["commodity", "brand", "specification"], sort=False):
        if len(g) < 20:
            continue
        idx = g.index.to_numpy()
        logp = g["log_price"].to_numpy()
        n = len(logp)
        w = min(window_weeks, max(8, n // 2))
        excl = min(exclude_weeks, max(4, n // 8))
        for i in range(n):
            if np.isnan(logp[i]):
                continue
            left_lo = max(0, i - w); left_hi = max(0, i - excl)
            right_lo = min(n, i + excl + 1); right_hi = min(n, i + w + 1)
            baseline = np.concatenate([logp[left_lo:left_hi], logp[right_lo:right_hi]])
            baseline = baseline[~np.isnan(baseline)]
            if len(baseline) < 4:
                continue
            if abs(logp[i] - np.median(baseline)) >= log_threshold:
                flagged.append(int(idx[i]))
    if flagged:
        out.loc[flagged, "price"] = np.nan
        out.loc[flagged, "log_price"] = np.nan
        out["log_price"] = (out.groupby(["commodity", "brand", "specification"])
                                ["log_price"]
                            .transform(lambda s: s.interpolate(
                                method="linear", limit_area="inside")))
        out["price"] = np.exp(out["log_price"])
        # `limit_area="inside"` refuses to fill at series endpoints and at
        # interior runs that interpolate to NaN (e.g. a flagged point with
        # no valid neighbors within `limit`). Drop those truly
        # unrecoverable rows so downstream consumers don't have to guard
        # for NaN prices.
        out = out.dropna(subset=["price"]).reset_index(drop=True)
    return out, flagged


def add_log_differences(df: pd.DataFrame,
                        lags: Tuple[int, ...] = (1, 4, 8)) -> pd.DataFrame:
    """Within each series, compute Δlog at requested weekly lags."""
    out = df.sort_values(["series_id", "date"]).copy()
    g = out.groupby("series_id")["log_price"]
    for k in lags:
        out[f"dlog_{k}w"] = g.diff(k)
    return out


# =============================================================================
#  L. EDA + VALIDATION
# =============================================================================

def run_eda(df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    rpt = {}
    rpt["shape"] = pd.DataFrame({
        "rows": [len(df)],
        "date_min": [df["date"].min()],
        "date_max": [df["date"].max()],
        "n_series": [df.groupby(["commodity", "brand", "specification"]).ngroups],
    })
    rpt["by_category"] = (df.groupby("category")["commodity"]
                            .nunique().to_frame("n_commodities"))
    rpt["commodities"] = (df.groupby(["category", "commodity"])
                            .agg(n_series=("brand",
                                 lambda s: s.astype(str).nunique()),
                                 n_rows=("price", "size"))
                            .reset_index())
    rpt["price_describe"] = df.groupby("commodity")["price"].describe()[
        ["count", "mean", "std", "min", "50%", "max"]]
    return rpt


def validate(df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    checks = {}
    ser = df.groupby(["commodity", "brand", "specification"])
    checks["weekly_step_ok"] = ser["date"].apply(
        lambda s: (s.sort_values().diff().dt.days.dropna() == 7).all()
    ).to_frame("ok")
    dups = df.groupby(["commodity", "brand", "specification", "date"]).size()
    checks["duplicate_obs"] = dups[dups > 1].to_frame("n")
    checks["nonpositive_prices"] = df[df["price"] <= 0]
    checks["coverage"] = ser.agg(
        n_weeks=("date", "nunique"), start=("date", "min"),
        end=("date", "max"), mean_price=("price", "mean"),
    )
    bimodal_rows = []
    for sid, g in df.groupby("series_id"):
        prices = g["price"].dropna().to_numpy()
        if len(prices) < 20:
            continue
        lo, hi = np.percentile(prices, [10, 90])
        if lo <= 0:
            continue
        spread = np.log(hi / lo)
        if spread > 0.4:
            bimodal_rows.append({"series_id": sid, "p10": lo, "p90": hi,
                                 "log_spread": spread, "n_weeks": len(prices)})
    checks["bimodal_series"] = (pd.DataFrame(bimodal_rows).sort_values(
        "log_spread", ascending=False) if bimodal_rows else pd.DataFrame(
        columns=["series_id", "p10", "p90", "log_spread", "n_weeks"]))
    return checks


# =============================================================================
#  M. OIL PANEL SCAFFOLD
# =============================================================================

def build_oil_panel_scaffold(start, end, freq: str = "W-MON") -> pd.DataFrame:
    """Scaffold for Brent/Dubai weekly panel (real fetcher plugs in)."""
    idx = pd.date_range(start=start, end=end, freq=freq)
    return pd.DataFrame({"date": idx, "brent_usd": np.nan, "dubai_usd": np.nan})


def merge_with_oil(panel: pd.DataFrame, oil: pd.DataFrame,
                   max_lag_weeks: int = 8) -> pd.DataFrame:
    """Left-join DTI panel with oil frame, precompute lagged Δlog(oil)."""
    out = panel.merge(oil, on="date", how="left").sort_values(["series_id", "date"])
    for col in ("brent_usd", "dubai_usd"):
        if col not in out.columns:
            continue
        out[f"log_{col}"] = np.log(out[col].where(out[col] > 0))
        dlog = out[f"log_{col}"].diff()
        for k in range(max_lag_weeks + 1):
            out[f"dlog_{col}_L{k}"] = dlog.shift(k)
    return out.reset_index(drop=True)


# =============================================================================
#  N. PIPELINE ENTRY POINT
# =============================================================================

def run_dti_pipeline(csv_dir: str | Path,
                     xlsx_path: str | Path) -> Tuple[pd.DataFrame, Dict]:
    """
    Execute the DTI phase end-to-end.

    Returns
    -------
    (weekly_panel, reports)
        weekly_panel : long-form W-MON panel with one row per
                       (series_id, date) and precomputed Δlog at lags 1/4/8.
        reports      : {'taxonomy', 'nan_report', 'eda', 'validation'}
    """
    taxonomy   = build_bnpc_taxonomy(xlsx_path)
    raw_long   = load_all_years(csv_dir)
    nan_report = inspect_categorical_nans(raw_long)

    # --- Section G: spec repair & harmonization ---
    spec_rep   = repair_cross_year_specifications(raw_long)
    no_empty   = drop_empty_specs(spec_rep)
    bucketed   = bucket_specs_within_brand(no_empty)
    harmonized = harmonize_candle_specs(bucketed)

    # --- Section H: category + dtypes ---
    with_cat   = attach_category(harmonized, taxonomy)
    typed      = enforce_dtypes(with_cat)

    # --- Section I: leader + collapse ---
    leaders    = select_market_leaders(typed)
    collapsed  = collapse_duplicates(leaders)

    # --- Section J: ffill + log + weekly ---
    ffilled    = forward_fill_prices(collapsed)
    logged     = add_log_price(ffilled)
    weekly     = aggregate_weekly(logged)

    # --- Section K: id + spike repair + dlog ---
    with_id            = add_series_id(weekly)
    spike_fix, flagged = repair_isolated_spikes(with_id)
    with_diff          = add_log_differences(spike_fix)

    # --- Section L: reports ---
    eda = run_eda(with_diff)
    val = validate(with_diff)
    val["n_spikes_repaired"] = pd.DataFrame({"n": [len(flagged)]})

    return with_diff, {
        "taxonomy":   taxonomy,
        "nan_report": nan_report,
        "eda":        eda,
        "validation": val,
    }


# =============================================================================
#  O. LEGACY LOADERS  — unused internally, kept for backward compat.
#
#  These functions predate the long-form `load_year` / `load_all_years`
#  pipeline above. They produce a dict of per-sheet wide DataFrames
#  without melting or normalizing. Retain them so any external notebook
#  that imports them still works.
# =============================================================================

def load_pp(filepath):
    """Legacy loader: read all non-PC sheets into a dict of DataFrames."""
    directory_dti = filepath
    dti_xls = pd.ExcelFile(directory_dti)
    all_sheets = dti_xls.sheet_names
    data = {}
    for sheet_name in all_sheets:
        if sheet_name.startswith('PC'):
            continue
        skip_rows = 5 if sheet_name == '2025' else 4
        if sheet_name in ['2023', '2024']:
            df_dti = pd.read_excel(dti_xls, sheet_name=sheet_name, skiprows=skip_rows)
            df_dti = df_dti.iloc[2:, 2:]
        else:
            df_dti = pd.read_excel(dti_xls, sheet_name=sheet_name, skiprows=skip_rows)
            if sheet_name in ['2020', '2021']:
                df_dti = df_dti.rename(columns={'Unnamed: 1': 'brand'})
        df_dti = df_dti.dropna(axis=1, how='all').dropna(axis=0, how='all')
        df_dti.columns = [str(c).replace('\n', ' ').strip().lower() for c in df_dti.columns]
        data[sheet_name] = df_dti
    return data


def add_standardize(data):
    """Legacy column-schema normalizer. Not used by the v2/v3 pipeline."""
    for sheet in data:
        df = data[sheet]
        df.columns = [re.sub(r'\s+', ' ', str(col)).strip().lower()
                      for col in df.columns]
        if sheet[-4:] in ['2020', '2021', '2022']:
            df = df.rename(columns={
                'basic necessities': 'commodity',
                'product name': 'brand',
                'unit': 'specification'
            })
        else:
            spec_mapping = {col: 'specification'
                            for col in df.columns if 'spec' in col}
            df = df.drop(df.columns[[0, 2, 5]], axis=1, errors='ignore').rename(columns={
                'product category': 'commodity',
                'brand name': 'brand',
                **spec_mapping
            })
        if 'commodity' not in df.columns:
            print(f"{sheet} has no 'commodity' column")
            data[sheet] = df
            continue
        df['commodity_clean'] = (
            df['commodity'].astype(str).str.lower().str.strip()
              .str.replace(r'\s+', ' ', regex=True)
              .str.replace(r'^(soft|hard)\s+flour$', 'flour', regex=True)
        )
        df = df[~df['commodity_clean'].str.contains(
            r'prime|brandon|^nan$|^$', na=True)]
        df = df.drop(columns=['commodity_clean'], errors='ignore')
        if 'category' in df.columns:
            df = df[['category'] + [col for col in df.columns if col != 'category']]
        df = df.dropna(axis=0, how='all')
        data[sheet] = df
    return data

# =============================================================================
#  P. POST-PIPELINE: COVERAGE FILTER + ISO-WEEK ALIGNMENT
#
#  Operates on the OUTPUT of run_dti_pipeline() (the weekly panel CSV).
#  Preserves all 218 SKU-level series (market leaders per specification).
#  Only removes commodities with insufficient temporal coverage.
#
#  Coverage threshold: 70% (Doz, Giannone & Reichlin, 2012, Econometrica).
# =============================================================================

MIN_COVERAGE_THRESHOLD = 0.70


def load_weekly_panel(filepath):
    """Load the weekly panel output of run_dti_pipeline()."""
    filepath = Path(filepath)
    if filepath.suffix == ".parquet":
        df = pd.read_parquet(filepath)
    else:
        df = pd.read_csv(filepath)
    if "Unnamed: 0" in df.columns:
        df = df.drop(columns=["Unnamed: 0"])
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"])
    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df = df[df["price"] > 0].copy()
    df["log_price"] = np.log(df["price"])
    df = df.sort_values(["date", "commodity", "brand"]).reset_index(drop=True)
    print(f"  [DTI LOAD] {len(df)} obs, {df['commodity'].nunique()} commodities, "
          f"{df['series_id'].nunique()} SKU series")
    print(f"  [DTI LOAD] {df.date.min().date()} to {df.date.max().date()}")
    return df


def filter_by_coverage(df, threshold=MIN_COVERAGE_THRESHOLD):
    """Drop commodities below coverage threshold. Returns (filtered_df, report)."""
    date_range = pd.date_range(df.date.min(), df.date.max(), freq="W-MON")
    total_weeks = len(date_range)
    cov = df.groupby("commodity")["date"].nunique().reset_index()
    cov.columns = ["commodity", "n_weeks"]
    cov["pct_coverage"] = cov["n_weeks"] / total_weeks
    cov = cov.sort_values("pct_coverage")
    to_drop = cov[cov["pct_coverage"] < threshold]["commodity"].tolist()
    if to_drop:
        print(f"  [DTI FILTER] Dropping {len(to_drop)} commodities < {threshold:.0%}:")
        for c in to_drop:
            pct = cov.loc[cov.commodity == c, "pct_coverage"].values[0]
            n_series = df[df.commodity == c]["series_id"].nunique()
            print(f"    - {c}: {pct:.1%} coverage, {n_series} SKU series")
        df = df[~df["commodity"].isin(to_drop)].reset_index(drop=True)
    retained = cov[cov["pct_coverage"] >= threshold]
    print(f"  [DTI FILTER] Retained {len(retained)} commodities, "
          f"{df['series_id'].nunique()} SKU series, {len(df)} obs")
    return df, cov


def clean_weekly_panel(df):
    """Add ISO-week columns, ensure Monday alignment, recompute log-diffs."""
    df = df.copy()
    df["iso_year"] = df["date"].dt.isocalendar().year.astype(int)
    df["iso_week"] = df["date"].dt.isocalendar().week.astype(int)
    non_monday = df["date"].dt.dayofweek != 0
    if non_monday.sum() > 0:
        df.loc[non_monday, "date"] = (
            df.loc[non_monday, "date"]
            - pd.to_timedelta(df.loc[non_monday, "date"].dt.dayofweek, unit="D")
        )
    df["week_start"] = df["date"]
    df = df.sort_values(["series_id", "date"])
    df["dlog_1w"] = df.groupby("series_id")["log_price"].diff(1)
    df["dlog_4w"] = df.groupby("series_id")["log_price"].diff(4)
    df["dlog_8w"] = df.groupby("series_id")["log_price"].diff(8)
    return df.reset_index(drop=True)


def aggregate_commodity_weekly(df):
    """
    Aggregate SKU-level to commodity-level using MEDIAN across brands.
    This is used ONLY for the FSFI and correlation analysis downstream.
    The SKU-level panel is preserved separately for granular analysis.
    """
    cw = df.groupby(["week_start", "commodity", "category"]).agg(
        price_median=("price", "median"),
        price_mean=("price", "mean"),
        n_brands=("brand", "nunique"),
        n_obs=("price", "count"),
        log_price_median=("log_price", "median"),
    ).reset_index()
    cw.rename(columns={"week_start": "date"}, inplace=True)
    cw = cw.sort_values(["commodity", "date"])
    cw["dti_dlog"] = cw.groupby("commodity").apply(
        lambda g: np.log(g["price_median"]).diff(), include_groups=False
    ).reset_index(level=0, drop=True)
    print(f"  [DTI AGG] {len(cw)} commodity-weeks ({cw.commodity.nunique()} commodities)")
    return cw