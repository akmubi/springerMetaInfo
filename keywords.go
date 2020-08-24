package main

import (
	"github.com/akmubi/soup"
	"strings"
)

func parseKeywords(url string) (keywords []string, err error) {
	response, err := soup.Get(url)
	if err != nil {
		return nil, err
	}

	document := soup.HTMLParse(response)
	divs := document.FindAll("div")
	for _, div := range divs {
		if div.Attrs()["class"] == "KeywordGroup" {
			spans := div.FindAll("span")
			for _, span := range spans{
				if span.Attrs()["class"] == "Keyword" {
					keywords = append(keywords, strings.ToLower(strings.TrimSuffix(span.FullText(), "&nbsp;")))
				}
			}
			return keywords, nil
		}
		if div.Attrs()["class"] == "c-bibliographic-information__column" {
			lists := div.FindAll("ul")
			for _, list := range lists {
				if list.Attrs()["class"] == "c-article-subject-list" {
					listItems := list.FindAll("li")
					for _, li := range listItems {
						keywords = append(keywords, strings.ToLower(li.FullText()))
					}
					return keywords, nil
				}
			}
		}		
	}

	return nil, nil
}