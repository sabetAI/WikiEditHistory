from wiki.revision_iterator import RevisionIterator
from wiki_edits.edits_extractor import EditsExtractor


class WikiEditsExtractor:
    def __init__(self, filename, **kwargs):
        self.revision = RevisionIterator(filename)
        self.extractor = EditsExtractor(**kwargs)

    def extract_edits(self):
        for old_text, new_text, info in self.__revision_pair():
            edits = self.extractor.extract_edits(old_text, new_text)
            if edits:
                yield (edits, info)

    def __revision_pair(self):
        for old_rev, new_rev in self.revision.adjacent_revisions():
            if 'text' in old_rev and 'text' in new_rev:
                old_text = old_rev['text']
                new_text = new_rev['text']

                new_rev.pop('text', None)
                yield (old_text, new_text, new_rev)
