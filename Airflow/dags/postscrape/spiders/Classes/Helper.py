import itertools


class Helper:
    
    def __init__(self) -> None:
        self.current_id = 0

    def fix_accuracy_lists(self, lst):
        """Accuracy lists often times contain missing information. This function adds
            blank strings for missing information.

        Args:
            lst list: The list to be fixed.

        Returns:
            list: The updated list.
        """
        updated_list = []
        final = ""
        for curr, next in self.pairwise(lst):
            final = next
            next_alpha = next.replace(" ", "").isalpha()
            curr_alpha = curr.replace(" ", "").isalpha()
            
            if next_alpha and curr_alpha:
                updated_list.append(curr)
                updated_list.append("")
            else:
                updated_list.append(curr)
                
        updated_list.append(final)
        if(final.isalpha()):
            updated_list.append("0")

        return updated_list

    def fix_base_lists(self, cleaned):
        """Base lists often contain missing information. This function will add
            blank strings depending on what information is missing. If something 
            important is missing or can't be calculated, it will return a non full list
            (a list that != 16) so it will not be added to the figther dictionary.

        Args:
            cleaned list: A cleaned list.

        Returns:
            list: An updated list if applicable, otherwise will return the same list.
        """
        updated_list = []
        for i in range(0, len(cleaned) - 1):
            temp_curr = cleaned[i].replace(" ", "").replace(".","")
            temp_next = cleaned[i+1].replace(" ", "").replace(".","")
            
            if temp_curr.isalpha() and temp_next.isalpha():
                if temp_next == "TakedownDefense":
                    updated_list.append(cleaned[i])
                    updated_list.append("")
                else:
                    updated_list.append(cleaned[i])
            else:
                updated_list.append(cleaned[i])
            
        updated_list.append(cleaned[-1])

        return updated_list

    def pairwise(self, iterable):
        """Helper function for interating the current and next item in a list.

        Args:
            iterable list: list to get the current and next item

        Returns:
            iterator: iterator to be used in the function.
        """
        a,b = itertools.tee(iterable)
        next(b,None)
        return zip(a,b)

    def swap(self, lst):
        """Swaps every pair of elements in a list. This is a helper function
            for adding stats to the fighter dictionary.

        Args:
            lst list: list that needs to have the elements swapped.

        Returns:
            list: Updated list with the swapped elements
        """
        for i in range(0, len(lst) - 1, 2):
            lst[i], lst[i + 1] = lst[i + 1], lst[i]
        return lst

    def list_to_dict(self, data):
        """Function to make a list into a dictionary.

        Args:
            data list: list to made into a dictionary

        Returns:
            dictionary: Dictionary to be used for fighter data.
        """
        value = iter(data)
        res_dct = dict(zip(value, value))
        
        return res_dct

    def clean_list(self, values):
        """Clean the list of empty white space and tabs

        Args:
            values list: List to be cleaned.

        Returns:
            list: The cleaned list.
        """
        res = [val.strip().replace('\n','') for val in values if val != '']

        return res

    def lists_to_dict(self, labels, values):
        """Make two lists of labels and values into one dictionary. 

        Args:
            labels list: List of labels.
            values list: List of values.

        Returns:
            dictionary: Dictionary made up from the lists.
        """
        res = {}
        for label, val in zip(labels, values):
            res[label] = val
        return res

    def fight_stats(self, response, name):
        """Finds the amount of wins and fights for each fighter. Due to how the UFC has the
            wins, it is easier to split up between red and blue to get all of the wins. 

        Args:
            response: Response to the current page.
            name string: name of the current fighter 

        Returns:
            tuple: the amount of fights and wins.
        """
        
        total_fights = response.css('.c-card-event--athlete-results__results')
        
        fights_blue = response.css('.c-card-event--athlete-results__headline::text , .c-card-event--athlete-results__blue-image .win::text').extract()
        fights_red = response.css('.c-card-event--athlete-results__headline::text , .c-card-event--athlete-results__red-image .win::text').extract()
        fights_blue = [blue.strip() for blue in fights_blue]
        fights_red = [red.strip() for red in fights_red]
        total_wins = 0

        for curr,nxt in self.pairwise(fights_blue):
            if curr == "Win":
                right_name = nxt.split(" ")[2]
                if right_name == name:
                    total_wins +=1
              
        for curr,nxt in self.pairwise(fights_red):           
            if curr == "Win":            
                left_name = nxt.split(" ")[0]      
                if left_name == name:
                        total_wins += 1

 
      
        return len(total_fights), total_wins



    def reset_data(self):
        """Creates a clean, formatted dictionary for a new fighter.

        Returns:
            dictionary: The formatted dictionary.
        """

        data = {
            "id": self.current_id,
            "first_name":"",
            "last_name":"",
            "Division":"",
            "Status":"",
            "Place_of_Birth":"",
            "Fighting_style":"",
            "Trains_at":"",
            "Octagon_Debut":"",
            "Sig_Strikes_Landed": "",
            "Sig_Strikes_Attempted":"",
            "Sig_Str_Landed": "",
            "Sig_Str_Absorbed": "",
            "Sig_Str_Defense": "",
            "Knockdown_Avg": "",
            "Standing":"",
            "Clinch":"",
            "Ground":"",
            "Sig_Str_Head":"",
            "Sig_Str_Body":"",
            "Sig_Str_Leg":"",
            "Takedowns_Landed":"",
            "Takedowns_Attempted":"",
            "Takedown_avg":"",
            "Takedown_Defense":"",
            "Submission_avg":"",
            "KO_TKO":"",
            "DEC":"",
            "SUB":"",
            "Reach":"",
            "Leg_reach":"",
            "Age":"",
            "Height":"",
            "Average_fight_time":"",
            "Fights":0,
            "Wins":0
        }

        self.current_id +=1

        


        return data