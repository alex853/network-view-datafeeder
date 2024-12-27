package net.simforge.networkview.datafeeder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/")
public class StatusController {
    private static final Logger logger = LoggerFactory.getLogger(StatusController.class);

    @RequestMapping("/hello-world")
    public String getHelloWorld() {
        return "Hello, World!";
    }
}
