from django.test import TestCase, Client
from django.urls import reverse


class IndexPageTest(TestCase):
    def test_index_renders(self):
        c = Client()
        response = c.get('/')

        self.assertEqual(response.status_code, 200)
        self.assertTemplateUsed(response, 'index.html')

    def test_index_links_public_endpoints(self):
        response = Client().get(reverse('index'))

        for name in ['server_count', 'version_breakdown',
                     'architecture_breakdown', 'feature_count', 'os']:
            self.assertContains(response, f'href="{reverse(name)}"')
        self.assertContains(response, 'http://testserver/rest/v1/post')

    def test_index_hides_protected_endpoints(self):
        response = Client().get(reverse('index'))

        for name in ['file_post_protected', 'get_uploads']:
            self.assertNotContains(response, reverse(name))
        self.assertNotContains(response, '/admin/')

    def test_index_rejects_post(self):
        response = Client().post(reverse('index'))

        self.assertEqual(response.status_code, 405)
