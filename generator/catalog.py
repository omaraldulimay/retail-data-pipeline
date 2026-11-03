from dataclasses import dataclass


@dataclass(frozen=True)
class Product:
    sku: str
    name: str
    category: str
    color: str
    price: float
    cost: float
    return_rate: float


# (base sku, name, category, price, cost, return rate, colours)
_BASE = [
    (
        "CNDL-AMB",
        "Duftkerze Bernstein",
        "Kerzen",
        24.90,
        7.10,
        0.03,
        ["Natur", "Rauch"],
    ),
    ("CNDL-FIG", "Duftkerze Feige", "Kerzen", 24.90, 7.10, 0.03, ["Natur", "Salbei"]),
    (
        "CNDL-TAP",
        "Stabkerzen 4er-Set",
        "Kerzen",
        12.90,
        3.40,
        0.02,
        ["Creme", "Terracotta", "Olive"],
    ),
    (
        "CER-MUG",
        "Steingut-Becher",
        "Keramik",
        18.50,
        5.20,
        0.06,
        ["Sand", "Moos", "Kobalt"],
    ),
    ("CER-BWL", "Müslischale Elbe", "Keramik", 16.90, 4.80, 0.06, ["Sand", "Moos"]),
    (
        "CER-VAS",
        "Vase Hafenkante",
        "Keramik",
        39.00,
        11.50,
        0.08,
        ["Kreide", "Anthrazit"],
    ),
    (
        "TXT-THR",
        "Wolldecke Nordwind",
        "Textilien",
        89.00,
        31.00,
        0.14,
        ["Grau", "Senf", "Rost"],
    ),
    (
        "TXT-CSH",
        "Kissenhülle Leinen",
        "Textilien",
        29.90,
        8.90,
        0.12,
        ["Natur", "Salbei", "Rost"],
    ),
    (
        "TXT-TWL",
        "Waffelhandtuch",
        "Textilien",
        19.90,
        5.60,
        0.10,
        ["Weiß", "Sand", "Moos"],
    ),
    (
        "DEC-MIR",
        "Spiegel Bullauge",
        "Deko",
        129.00,
        48.00,
        0.09,
        ["Messing", "Schwarz"],
    ),
    ("DEC-FRM", "Bilderrahmen Eiche", "Deko", 34.00, 10.20, 0.07, ["A4", "A3"]),
    (
        "DEC-LMP",
        "Tischleuchte Leuchtturm",
        "Deko",
        149.00,
        56.00,
        0.10,
        ["Weiß", "Schwarz"],
    ),
    ("KIT-BRD", "Schneidebrett Esche", "Küche", 44.00, 13.80, 0.04, ["Klein", "Groß"]),
    (
        "KIT-TEA",
        "Teekanne Speicherstadt",
        "Küche",
        54.00,
        17.00,
        0.05,
        ["Sand", "Kobalt"],
    ),
    ("KIT-LIN", "Geschirrtuch-Set", "Küche", 22.00, 6.10, 0.06, ["Streifen", "Karo"]),
]


def build_catalog() -> list[Product]:
    products = []
    for base, name, category, price, cost, return_rate, colours in _BASE:
        for colour in colours:
            code = colour.upper().replace("Ä", "AE").replace("Ö", "OE").replace("Ü", "UE")
            products.append(
                Product(
                    sku=f"{base}-{code[:4]}",
                    name=f"{name} {colour}",
                    category=category,
                    color=colour,
                    price=price,
                    cost=cost,
                    return_rate=return_rate,
                )
            )
    return products
