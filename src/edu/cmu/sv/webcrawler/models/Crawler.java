package edu.cmu.sv.webcrawler.models;

import edu.cmu.sv.webcrawler.services.Get10K;

public class Crawler {

	public void crawl(String symbol, String documentType){
		Get10K g=new Get10K();
		g.Download10KbyCIK(symbol, false, documentType);
	}
}
