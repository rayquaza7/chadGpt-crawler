// For more information, see https://crawlee.dev/
import { PlaywrightCrawler, EnqueueStrategy } from "crawlee";
import PG from "pg";

const pool = new PG.Pool();

// PlaywrightCrawler crawls the web using a headless
// browser controlled by the Playwright library.
const crawler = new PlaywrightCrawler({
  // Use the requestHandler to process each of the crawled pages.
  async requestHandler({ request, page, enqueueLinks, log }) {
    const client = await pool.connect();

    const title = await page.title();
    const text = await page.innerText("body");
    log.info(`Title of ${request.loadedUrl} is '${title}'`);
    // check if the url is already in the database
    const res = await client.query("SELECT * FROM crawl_data WHERE url = $1", [
      request.loadedUrl,
    ]);
    if (res.rowCount > 0) {
      console.log("Already in database", request.loadedUrl);
    } else {
      try {
        await client.query(
          "INSERT INTO crawl_data (url, title, data, company_id) VALUES ($1, $2, $3, $4)",
          [request.loadedUrl, title, text, 1]
        );
      } catch (e) {
        console.log(
          "Could not add to database",
          e,
          request.loadedUrl,
          title,
          text,
          1
        );
      }
    }
    client.release();

    // Extract links from the current page
    // and add them to the crawling queue.
    await enqueueLinks({
      strategy: EnqueueStrategy.SameDomain,
      transformRequestFunction(req) {
        // ignore all links ending with `.pdf`
        if (req.url.endsWith(".pdf")) return false;
        // ignore all links that have date in the, e.g. /2021/01/01/
        if (req.url.match(/\/\d{4}\/\d{2}\/\d{2}\//)) return false;
        return req;
      },
    });
  },
  // Uncomment this option to see the browser window.
  // headless: false,
});

// Add first URL to the queue and start the crawl.
// await crawler.run(["https://www.ubc.ca/"]);
