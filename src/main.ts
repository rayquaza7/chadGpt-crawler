import { EnqueueStrategy, PuppeteerCrawler } from "crawlee";
import PG from "pg";

const client = new PG.Client();
await client.connect();

// PlaywrightCrawler crawls the web using a headless
// browser controlled by the Playwright library.
const crawler = new PuppeteerCrawler({
  // Use the requestHandler to process each of the crawled pages.
  async requestHandler({ request, page, enqueueLinks, log }) {
    const title = await page.title();
    // get all text from p tags
    const text = await page.evaluate(() => {
      const elements = document.querySelectorAll("p");
      const text = [];
      for (let i = 0; i < elements.length; i++) {
        text.push(elements[i].innerText);
      }
      return text;
    });
    log.info(`Crawled ${request.loadedUrl} (title: ${title})`);

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

    // Extract links from the current page
    // and add them to the crawling queue.
    await enqueueLinks({
      strategy: EnqueueStrategy.SameDomain,
      transformRequestFunction(req) {
        // ignore all links ending with `.pdf`
        if (req.url.endsWith(".pdf")) return false;
        // ignore all links that have date in the, e.g. /2021/01/01/
        if (req.url.match(/\/\d{4}\/\d{2}\/\d{2}\//)) return false;
        // ignore all links if they have the word 'recreation' in them
        if (req.url.match(/recreation/)) return false;
        // ignore all links if they have the word 'give' in them
        if (req.url.match(/give/)) return false;
        // ignore library links
        if (req.url.match(/library/)) return false;
        // ignnore .ok subdomains
        if (req.url.match(/.ok/)) return false;
        return req;
      },
    });
  },
});

// Add first URL to the queue and start the crawl.
await crawler.run([
  "https://courses.students.ubc.ca/cs/courseschedule?pname=subjarea&tname=subj-all-departments",
]);
