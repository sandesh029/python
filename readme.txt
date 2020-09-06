abandoned matches are left out and not included

How to run

->  first start the kafka zookepeer and then kafka server using command prompt
->  run the consumer.py first then run the producers.py


output

Q-2 a)   first in the set of files you will see only files like "194161014-4148_com.txt"
         these are commentary files that are available to producers

        
on the console of producers.py you can see the statement like
194161014-4184     2
194161014-4187     END OF OVER:35 | 4 Runs | INDIA: 211/1 (54 runs required from 15 overs, RR: 6.02, RRR: 3.60)
194161014-4182     Jasprit Bumrah is on song at the moment - when isn't he? Good time to revisit Nagraj Gollapudi's tale about Bumrah

these statements are match id following the commentary in the sequence these are in kafka server

on opening the file "interleaving commentary sequence.txt"  or looking at the console it is clear that it is interleaving in nature only when one match is left it will stream single match otherwise commentary from multiple matches are send to kafka server.


   b)
           
        on running the code of consumer it will create a file like "194161014-4148_com(kafka).txt"
  these are the files that are created by different consumers by listening to their respective topics  you can the length of this is same as its producer counterpart.

on opening the file "message sequence consumer.txt"  it tells us different consumers listening to their topics in interleaved fashion.


c)  in both the above questions streams are handled in interleaved fashion
interleaving commentary sequence.txt
message sequence consumer.txt
  
 can be observed using these text files


d) for generation of score card  i have choosen 3 matches of id 194161014-4148,194161014-4158 
    the generation of score can take 40 minutes 
so i am showing this for these two match-ids only

you will find files like this "194161014-4148_scorecardsequence.txt"
this will give you the sequence of score card updated for every line of commentary of that match

you will find file like "194161014-4148_finalscorecard.txt"
when the console stop printing this file will give you the final scorecard of that match


on console you will see the updated scorecard commentary by commentary for these 2 matches 







