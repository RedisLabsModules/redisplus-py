from enum import Enum


class IndexType(Enum):
    """
    Enum of the currently supported index types.
    """

    HASH = 1
    JSON = 2


class IndexDefinition(object):
    """
    IndexDefinition is used to define a index definition for automatic indexing on Hash or Json update.
    """

    def __init__(
        self,
        prefix=[],
        filter=None,
        language_field=None,
        language=None,
        score_field=None,
        score=1.0,
        payload_field=None,
        index_type=None,
    ):
        args = []

        if index_type is IndexType.HASH:
            args.extend(["ON", "HASH"])
        elif index_type is IndexType.JSON:
            args.extend(["ON", "JSON"])
        elif index_type is not None:
            raise RuntimeError("index_type must be one of {}".format(list(IndexType)))

        if len(prefix) > 0:
            args.append("PREFIX")
            args.append(len(prefix))
            for p in prefix:
                args.append(p)

        if filter is not None:
            args.append("FILTER")
            args.append(filter)

        if language_field is not None:
            args.append("LANGUAGE_FIELD")
            args.append(language_field)

        if language is not None:
            args.append("LANGUAGE")
            args.append(language)

        if score_field is not None:
            args.append("SCORE_FIELD")
            args.append(score_field)

        if score is not None:
            args.append("SCORE")
            args.append(score)

        if payload_field is not None:
            args.append("PAYLOAD_FIELD")
            args.append(payload_field)

        self.args = args
