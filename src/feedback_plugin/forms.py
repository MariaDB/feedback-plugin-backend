from django import forms

class UploadFileForm(forms.Form):
  data  = forms.FileField()