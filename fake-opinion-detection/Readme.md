# Fake Opinion Detection

For any organization, online customer reviews are considered as the most useful sources of information which crafts its reputation. While making an online purchase, from a buyers’ perspective, the key to making the right decision to buy depends solely on other customers’
reviews. Recently, online review platforms are being illegally exploited by spammers forcing to write fake reviews because of incentives involved thereby gaining an edge over
competitors which results in an explosive growth of spamming. The primary problem statement was to detect fake reviews from authentic ones.

Readers are misguided by opinion spammers who try to tamper the automatic systems designed for opinion mining and sentiment analysis by providing undeserving positive
opinions in order to promote the entities by giving incorrect negative opinions to some other entities in order to hamper their reputations.

### Dataset

We are using Yelp dataset to build our Fake Opinion Detector in which we have taken up
specific NYC dataset which contains the reviews of restaurants and hotels of NYC. 

Data source: http://shebuti.com/wp-content/uploads/2016/06/15-kdd-collectiveopinionspam.pdf

Our dataset contains 359,052 reviews out of which 10.27% data is fake. These reviews are
given by 160,225 users for 923 products. We have the following two files for our dataset: metadata.txt, reviewContent.txt

Data format :

a. metadata file: “user_id”, “product_id”, “rating”, “label”, “date”

b. reviewContent file: "user_id","prod_id","date","review"

Hence these 2 files are our input. We have merged these two files based on user_id,
product_id and date in our program to get the final dataset.


### Implementation Details

#### a. Preprocessing:

1. We merged the data from the two files into a single dataframe using the common features: user_id, prod_id and date.
2. Punctuation removal.
3. We observed that there was very less fake content i.e. around 10.27% which
resulted in class imbalance. Hence, we down-sampled the majority content and brought down
the ratio of authentic data and fake data to 2.
4. Stop words removal.
5. Lemmatization.

#### b. Feature Engineering


