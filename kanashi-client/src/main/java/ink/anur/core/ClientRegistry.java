//package ink.anur.core;
//
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.context.annotation.Bean;
//import org.springframework.context.annotation.Configuration;
//import ink.anur.common.Constant;
//import ink.anur.common.struct.KanashiNode;
//import ink.anur.config.KanashiConfig;
//import ink.anur.core.client.ClientOperateHandler;
//
///**
// * Created by Anur IjuoKaruKas on 2020/3/4
// */
//@Configuration
//public class ClientRegistry {
//
//    @Autowired
//    private KanashiConfig kanashiConfig;
//
//    @Bean
//    public ClientOperateHandler clientOperateHandler() {
//        ClientOperateHandler handler = new ClientOperateHandler(new KanashiNode(Constant.INSTANCE.getSERVER(), kanashiConfig.getHost(), kanashiConfig.getPort()));
//        handler.start();
//        return handler;
//    }
//}
