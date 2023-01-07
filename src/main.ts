// For more information, see https://crawlee.dev/
import { PlaywrightCrawler, EnqueueStrategy } from "crawlee";
import { Pool, Client } from "pg";

const client = new Client({
  connectionString: process.env.DATABASE_URL,
});
await client.connect();
const pool = new Pool();

// PlaywrightCrawler crawls the web using a headless
// browser controlled by the Playwright library.
const crawler = new PlaywrightCrawler({
  // Use the requestHandler to process each of the crawled pages.
  async requestHandler({ request, page, enqueueLinks, log }) {
    const title = await page.title();
    const text = await page.innerText("body");
    log.info(`Title of ${request.loadedUrl} is '${title}'`);

    // Extract links from the current page
    // and add them to the crawling queue.
    await enqueueLinks({
      strategy: EnqueueStrategy.SameDomain,
    });
  },
  // Uncomment this option to see the browser window.
  // headless: false,
});

// Add first URL to the queue and start the crawl.
// await crawler.run(["https://www.ubc.ca/"]);
