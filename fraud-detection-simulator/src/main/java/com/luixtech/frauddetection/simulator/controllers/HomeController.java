package com.luixtech.frauddetection.simulator.controllers;

import java.io.IOException;
import javax.servlet.http.HttpServletResponse;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class HomeController {

  /** Home page. */
  @GetMapping("/")
  public void home(HttpServletResponse response) throws IOException {
    response.sendRedirect("swagger-ui.html");
  }
}
