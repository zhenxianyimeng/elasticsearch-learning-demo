package com.zx.learn.es.client.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.NonFinal;

import java.util.Date;

/**
 * Description:
 *
 * @author: chixiao
 * @date: 2019-09-05
 * @time: 20:20
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Rating {
    private Long id;

    private String title;

    private Long userId;

    private Long productId;

    private Long rating;

    private Date create;

    private String comment;
}
