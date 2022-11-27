package com.ab.core;

import lombok.Getter;
import lombok.Setter;

/**
 * @description:
 * @version: 0.0.1
 * @author: liwenhui
 * @createTime: 2022-11-27 19:51
 **/
@Getter
@Setter
public class Article {

    private String id;
    private String title;
    private String author;
    private String describe;
    private String content;
    private String time;

    @Override
    public String toString() {
        return "Article{" +
                "id='" + id + '\'' +
                ", title='" + title + '\'' +
                ", author='" + author + '\'' +
                ", describe='" + describe + '\'' +
                ", content='" + content + '\'' +
                ", time='" + time + '\'' +
                '}';
    }
}
