# webpage_seo
A webpage scraper to help with SEO of it.


- There are three type of spiders are included
1. onpage; Pure scrapy spider
2. op_teer: Scrapy with splash support(lightweight browser based approach)
3. failed_test: For testing of failed URL.



[Note]
- This scraper help to get the details regarding the way of webpage is implemented to understand and improve it for SEO.
- This scrapr scrapes details including H1-H6 tags, internal links, external links, sitemap links, header/footer links, image links, canonicals, social links, broken links video counts, youtube links. This is just a basic version of scraper for SEO. This can be improved as much as you needed.
- The scraper is linked with Flask app which can be used to deploy on server to magage the scraper using API.