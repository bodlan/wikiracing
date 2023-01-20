import unittest

import wikipedia

from wikiracing import WikiRacer


class WikiRacerTest(unittest.TestCase):

    racer = WikiRacer()
    links_per_page = 200

    def test_1(self):
        path = self.racer.find_path("Дружба", "Рим")
        self.assertEqual(path, ["Дружба", "Якопо Понтормо", "Рим"])

    def test_2(self):
        path = self.racer.find_path("Мітохондріальна ДНК", "Вітамін K")
        self.assertEqual(path, ["Мітохондріальна ДНК", "Бактерії", "Вітамін K"])

    def test_3(self):
        path = self.racer.find_path("Марка (грошова одиниця)", "Китайський календар")
        # NOTE: Any year article have a link to 'Китайський календар', so it's hard to assert
        self.assertEqual(len(path), len(["Марка (грошова одиниця)", "2017", "Китайський календар"]))

    def test_4(self):
        path = self.racer.find_path("Фестиваль", "Пілястра")
        # NOTE: ['Фестиваль', 'Бароко', 'Пілястра'] is shortest path if unlimited links_per_page
        self.assertEqual(path, ["Фестиваль", "Бароко", "Архітектурний ордер", "Пілястра"])

    def test_5(self):
        path = self.racer.find_path("Дружина (військо)", "6 жовтня")
        self.assertEqual(path, ["Дружина (військо)", "Перша світова війна", "6 жовтня"])

    def test_retrieve_titles(self):
        titles = self.racer.retrieve_titles("Фестиваль")
        self.assertLessEqual(len(titles), self.links_per_page)

    def test_fail(self):
        with self.assertRaises(wikipedia.exceptions.PageError):
            self.racer.find_path("123AAA", "123AAB")

    def test_show_results(self):
        # NOTE: this test for results output
        self.racer.evaluate_db()
        self.assertLessEqual(self.racer.get_average_article_links("Бароко"), self.links_per_page)
        self.assertEqual(len(self.racer.get_routes(4)), 5)


if __name__ == "__main__":
    unittest.main()
