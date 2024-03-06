from scrapy import cmdline

cmdline.execute("scrapy crawl filmes -O /tmp/filmes.json".split())
