from __future__ import absolute_import

from dictshield.fields import BaseField


class ListField(BaseField):

    def __init__(self, field, **kwargs):
        self.field = field
        kwargs.setdefault("default", list)
        super(ListField, self).__init__(**kwargs)

    def to_python(self, value):
        return [self.field.to_python(item) for item in value]

    def validate(self, value):
        [self.field.validate(item) for item in value]

    def lookup_member(self, name):
        return self.field.lookup_member(name)

    @property
    def owner_document(self):
        return self._owner_document

    @owner_document.setter
    def owner_document(self, d):  # noqa
        self._owner_document = self.field.owner_document = d

