import pandas as pd


class BasicCrosser(object):
    def __init__(self) -> None:
        self.columns = ["cross_minus", "cross_plus", "cross_intersect"]

    def get_tokens(
        self,
        data: pd.DataFrame,
        col: str,
        dop_symbols: str,
    ) -> pd.Series:
        rx = f"[а-яa-z0-9][а-яa-z0-9{dop_symbols}]+"

        data["tokens"] = data[col].str.lower().str.findall(rx)
        data["tokens"] = data["tokens"].apply(lambda x: set(x))

        return data["tokens"]

    def get_tokens_pro(self, data: pd.DataFrame, col: str, extractors: list):
        data["tokens"] = [[] for _ in data.index]
        for extractor in extractors:
            data["tokens"] = data["tokens"] + extractor.extract(
                data, col, return_mode="series"
            )

        data["tokens"] = data["tokens"].apply(
            lambda words: [w.lower() for w in words],
        )
        data["tokens"] = data["tokens"].apply(lambda x: set(x))
        return data

    def get_cross_minus(self, current_set: set, other_set: set) -> set[str]:
        equation = current_set.symmetric_difference(other_set)

        if len(equation) == 1:
            equation = equation - current_set
            return equation
        return set()

    def get_cross_intersect(
        self,
        current_set: set,
        other_set: set,
    ) -> set[str]:
        equation = current_set.symmetric_difference(other_set)
        if len(equation) == 2:
            current_intersect = equation - current_set
            other_intersect = equation - other_set
            if (len(current_intersect) == 1) and (len(other_intersect) == 1):
                # Now we should swap it!
                return other_intersect, current_intersect
            return set()
        return set()
