# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class PostscrapeItem(scrapy.Item):
    # define the fields for your item here like:
    # stat_1 = scrapy.Field()
    # stat_2 = scrapy.Field()
    # stat_3 = scrapy.Field()

    Sig_Strikes_Landed = scrapy.Field()
    Sig_Strikes_Attempted = scrapy.Field()
    Sig_Str_Landed = scrapy.Field()
    Sig_Str_Absorbed = scrapy.Field()
    Sig_Str_Defense = scrapy.Field()
    Knockdown_Avg = scrapy.Field()
    Standing = scrapy.Field()
    Clinch = scrapy.Field()
    Ground = scrapy.Field()
    Sig_Str_Head = scrapy.Field()
    Sig_Str_Body = scrapy.Field()
    Sig_Str_Leg = scrapy.Field()
    Takedowns_Landed = scrapy.Field()
    Takedowns_Attempted = scrapy.Field()
    Takedown_avg = scrapy.Field()
    Takedown_Defense = scrapy.Field()
    Submission_avg = scrapy.Field()
    KO_TKO = scrapy.Field()
    DEC = scrapy.Field()
    SUB = scrapy.Field()
    Average_fight_time = scrapy.Field()
    Reach = scrapy.Field()
    Leg_reach = scrapy.Field()

    Status = scrapy.Field()
    Place_of_Birth = scrapy.Field()
    Fighting_style = scrapy.Field()
    Trains_at = scrapy.Field()
    Age = scrapy.Field()
    Height = scrapy.Field()
    Weight = scrapy.Field()
    Octagon_Debut = scrapy.Field()
    Division = scrapy.Field()
    first_name = scrapy.Field()
    last_name = scrapy.Field()
    Fights = scrapy.Field()



    
