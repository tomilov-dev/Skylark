class CONFIG(object):
    NAME = "config_name"

    REGEX_WEIGHTS = "regex_weights"
    LANGUAGE_WEIGHTS = "language_weights"
    RATIO = "ratio"
    WORD_MIN_LEN = "word_min_lenght"


class REGEX_WEIGHTS(object):
    CAPS = "caps"
    CAPITAL = "capital"
    LOW = "low"
    OTHER = "other"


class LANGUAGE_WEIGHTS(object):
    RUS = "rus"
    ENG = "eng"


class RATIO(object):
    MIN_RATIO = "min_ratio"
    MAX_RATIO = "max_ratio"
    MIN_APPEARANCE = "min_appearance"
    MIN_APPEARANCE_PENALTY = "min_appearance_penalty"
    RATE_FUNC = "rate_func"
