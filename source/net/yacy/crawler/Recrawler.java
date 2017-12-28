/**
 * Recrawler.java
 * Copyright 2017 by ScRe13 https://github.com/Scre13
 * First released 26.12.2017
 *
 * This is a part of YaCy, a peer-to-peer based web search engine
 *
 * LICENSE
 *
 * This library is free software; you can redistribute it and/or modify it under
 * the terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation; either version 2.1 of the License, or (at your option)
 * any later version.
 *
 * This library is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
 * details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program in the file lgpl21.txt If not, see
 * <http://www.gnu.org/licenses/>.
 */
package net.yacy.crawler;
import java.io.IOException;
import java.util.Date;
import net.yacy.cora.document.id.DigestURL;
import net.yacy.cora.util.ConcurrentLog;
import net.yacy.crawler.data.CrawlQueues;
import net.yacy.crawler.data.CrawlProfile;
import net.yacy.crawler.retrieval.Request;
import net.yacy.crawler.data.NoticedURL;
import net.yacy.search.Switchboard;
import net.yacy.search.SwitchboardConstants;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrException;
import net.yacy.cora.date.ISO8601Formatter;


public class Recrawler {

	// statics
	public static final ConcurrentLog log = new ConcurrentLog(Recrawler.class.getName());
	/** pseudo-random key derived from a time-interval while YaCy startup */
	public static long speedKey = 0;
	Switchboard sb;

	public Recrawler(final Switchboard sb) {
		final long time = System.currentTimeMillis();
		this.sb = sb;
		log.info("RECRWALER INITIALIZED");
		speedKey = System.currentTimeMillis() - time;
	}

	protected class publishThread extends Thread {
		@Override
		public final void run() {
			try {
				log.info("RECRWALER RUN");
			} catch (final Exception e) {
				ConcurrentLog.logException(e);
				log.severe("RECRWALER: error ");

			}
		}
	}

	
	public final void AddToQueue() {
		
		final CrawlQueues cq = sb.crawlQueues;
		int maxqueuesize = sb.getConfigInt(SwitchboardConstants.RECRAWLER_MAX_QUEUE_SIZE, 500);
		if (cq.coreCrawlJobSize() > maxqueuesize) {
			log.info("start condition not met, queue size too big " + cq.coreCrawlJobSize() + " > " + maxqueuesize + " can be changed in config: (" + SwitchboardConstants.RECRAWLER_MAX_QUEUE_SIZE +")");
			return;
		}
		log.info("start condition OK, queue size " + cq.coreCrawlJobSize() + " < " + maxqueuesize + " can be changed in config: (" + SwitchboardConstants.RECRAWLER_MAX_QUEUE_SIZE +")");
		
		sb.index.fulltext().commit(true);
		log.info("RECRWALER starting cycle to add URLs to be recrawled");
		
		String rows = sb.getConfig(SwitchboardConstants.RECRAWLER_ROWS, "1000");
		log.info("Rows to fetch in one cycle: " + rows + " can be changed in config: (" + SwitchboardConstants.RECRAWLER_ROWS +")");
		String days = "365"; // URLs last load > x days
		String dateQuery = String.format("fresh_date_dt:[* TO NOW/DAY-30DAY] AND load_date_dt:[* TO NOW/DAY-%sDAY]",
				days, days); // URLs which have a fresh date > 30 days and were loaded > x days ago

		final SolrQuery query = new SolrQuery();
		query.setQuery(dateQuery);

		query.setFields("sku");
		query.add("rows", rows);
		query.addSort("load_date_dt", SolrQuery.ORDER.asc);

		log.info("RECRWALER QUERY:" + query.toString());
		try {
			
			QueryResponse resp = sb.index.fulltext().getDefaultConnector().getResponseByParams(query);
			//log.info("RECRWALER RESPONSE:" + resp.toString());
			log.info("RECRWALER got " + query.getRows() + " rows from query");
			//ConcurrentLog.info(Recrawler.class.getName(), "RECRWALER RESPONSE:" + resp.toString());

			final CrawlProfile profile = sb.crawler.defaultTextSnippetGlobalProfile;
			
			int added = 0;
			Date now = new Date();
			
			for (SolrDocument doc : resp.getResults()) {

				DigestURL url;
				if (doc.getFieldValue("sku") != null) {

					final String u = doc.getFieldValue("sku").toString();
					if (doc.getFieldValue("httpstatus_i") != null) {
						//log.info("RECRWALER ADD: " + u + "with status: " + doc.getFieldValue("httpstatus_i").toString() );
					} else {
						log.info("RECRWALER ADD: " + u + "with status: NOSTATUS");
					}
					
					
					url = new DigestURL(u);
					final Request request = sb.loader.request(url, true, true);
	                String acceptedError = sb.crawlStacker.checkAcceptanceChangeable(url, profile, 0);
	                if (acceptedError == null) { // skip check if failed docs to be included
	                    acceptedError = sb.crawlStacker.checkAcceptanceInitially(url, profile);
	                }
	                if (acceptedError != null) {
	                	log.info("RECRWALER addToCrawler: cannot load " + url.toNormalform(true) + ": " + acceptedError);
	                    continue;
	                }
	                final String s;
	                s = sb.crawlQueues.noticeURL.push(NoticedURL.StackType.LOCAL, request, profile, sb.robots);

	                if (s != null) {
	                	log.info("RECRWALER addToCrawler: failed to add " + url.toNormalform(true) + ": " + s);
	                	sb.index.fulltext().remove(url.hash()); // If adding URL fails, delete it from index
	                } else {
	                    added++;
	                    
	                }
				} else {

				}
				sb.index.fulltext().commit(true);
			}
			//log.info("RECRWALER ADDED " + added + " URLs with timestamp: " + ISO8601Formatter.FORMATTER.format(now));
			ConcurrentLog.info(Recrawler.class.getName(), "RECRWALER ADDED " + added + " URLs with timestamp: " + ISO8601Formatter.FORMATTER.format(now));
		} catch (SolrException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return;
	}

}
