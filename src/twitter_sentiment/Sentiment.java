package twitter_sentiment;

public class Sentiment {

	private String search = null;
	private int rating;
    private String text = null;


    public Sentiment(String search, int rating, String text) {
    	this.search = search;
        this.text = text;
        this.rating = rating;
    }
    
    public Sentiment(String search, int rating) {
    	this.search = search;
        this.rating = rating;
    }

    public String getSearch() {
        return search;
    }

    public void setSearch(String search) {
        this.search = search;
    }

    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
    }
    
    public int getRating() {
        return rating;
    }

    public void setRating(int rating) {
        this.rating = rating;
    }

}
