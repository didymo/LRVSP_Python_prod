import bs4
import re


class reference:
    """
    Stores id, title value pair while only using id for comparison
    """
    id: str
    title: str

    def __init__(self, id: str, title: str):
        self.id = id
        self.title = title

    def __eq__(self, other):
        if self.id == other.id:
            return True
        else:
            return False

    def __hash__(self):
        return hash(id)


def getTitle(ref: bs4.PageElement) -> str:
    # get text from inside reference,
    # then replace all whitespace with single spaces,
    # then remove any 'no xyz' suffixes.
    return re.sub(r"no \d+$", r"",
                  re.sub(r"\s+", r" ",
                         "".join([x for x in ref.stripped_strings])),
                  flags=re.MULTILINE | re.IGNORECASE)


def process(xml: str) -> dict[str, dict, set]:
    with open(f"{xml}", 'r', encoding="utf8") as input:
        soup = bs4.BeautifulSoup(input, 'xml')
        metadata = {attrib["name"]: attrib["value"] for attrib in
                    soup.exdoc.parentattributes.find_all("attrib") if
                    attrib["value"] != ""}
        docId = metadata["id"]
        docTitle = metadata["title"]
        refs: set[reference] = set()
        for ref in soup.find_all("legref"):
            for name, value in ref.attrs.items():
                if "id" in name:
                    refs.add(reference(value, getTitle(ref)))
                    break
            else:
                title = getTitle(ref)
                refs.add(reference(title, title))

        # discard self references
        refs.discard(reference(docId, ""))
        refs.discard(reference(docTitle, ""))

        retDict = {
            "name": docTitle,
            "metadata": metadata,
            # links is only a set of titles, we don't care about the ids
            "links": {ref.title for ref in refs}
        }

        return retDict
