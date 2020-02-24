package ink.anur.inject

import ink.anur.exception.NoSuchBeanException
import org.slf4j.LoggerFactory
import java.io.File
import kotlin.jvm.internal.ClassReference
import kotlin.reflect.KClass
import kotlin.reflect.full.declaredMemberProperties
import kotlin.reflect.full.memberFunctions
import kotlin.reflect.jvm.isAccessible
import kotlin.reflect.jvm.javaField
import kotlin.reflect.jvm.javaType
import kotlin.system.exitProcess

/**
 * Created by Anur IjuoKaruKas on 2020/2/23
 *
 * 扫描系列
 */
object Nigate {

    private val defaultClassPath = this.javaClass.getResource("/").path

    private val logger = LoggerFactory.getLogger(this::class.java)

    private val BEAN_MAPPING = mutableMapOf<String, Any>()

    init {
        val scans = mutableSetOf<Class<*>>()
        doScan(scans, File(defaultClassPath))

        logger.info("Nigate ==> Registering..")
        for (clazz in scans) {
            register(clazz.newInstance(), clazz.simpleName)
        }
        logger.info("Nigate ==> Register complete")

        logger.info("Nigate ==> Injecting..")
        for (bean in BEAN_MAPPING.values) {
            inject(bean)
        }
        logger.info("Nigate ==> Inject complete")

        logger.info("Nigate ==> Invoking postConstruct..")
        for (bean in BEAN_MAPPING.values) {
            postConstruct(bean)
        }
        logger.info("Nigate ==> Invoke postConstruct complete")
    }

    fun getBeanByName(name: String): Any = BEAN_MAPPING[name] ?: throw NoSuchBeanException("bean named $name is not managed")

    fun <T> getBeanByClass(clazz: Class<T>): T = (BEAN_MAPPING[clazz.simpleName] ?: throw NoSuchBeanException("bean with type $clazz is not managed")) as T

    /**
     * 注册某个bean
     */
    fun register(bean: Any, name: String? = null) {
        val actualName = name ?: bean.javaClass.simpleName
        BEAN_MAPPING[actualName] = bean
        logger.debug("bean named [$actualName] is managed by Nigate")
    }

    /**
     * 为某个bean注入成员变量
     */
    fun inject(injected: Any) {
        for (kProperty in injected::class.declaredMemberProperties) {
            for (annotation in kProperty.annotations) {
                if (annotation.annotationClass == NigateInject::class) {
                    val injection = BEAN_MAPPING[(kProperty.returnType.javaType as Class<*>).simpleName]
                    val javaField = kProperty.javaField!!
                    javaField.isAccessible = true
                    javaField.set(injected, injection)
                }
            }
        }
    }

    fun postConstruct(bean: Any) {
        for (memberFunction in bean::class.memberFunctions) {
            for (annotation in memberFunction.annotations) {
                if (annotation.annotationClass == PostConstruct::class) {
                    try {
                        memberFunction.isAccessible = true
                        memberFunction.call(bean)
                    } catch (e: Exception) {
                        logger.error("class [${bean::class}] invoke post construct method [${memberFunction.name}] error : ${e.message}")
                        exitProcess(1)
                    }
                    break
                }
            }
        }
    }

    private fun doScan(scans: MutableSet<Class<*>>, file: File, packageName: String = "", start: Boolean = true) {
        if (file.isDirectory) {
            file.listFiles().forEach {
                val pn = if (start) {
                    packageName
                } else {
                    (packageName.takeIf { pn -> pn.isNotEmpty() }?.let { pn -> "$pn." } ?: "") + file.name
                }
                doScan(scans, it, pn, false)
            }
        } else if (file.name.endsWith(".class")) {
            Class.forName(((packageName.takeIf { it.isNotEmpty() }?.let { "$it." }) ?: "") + file.name.substring(0, file.name.lastIndexOf(".")))
                .takeIf { clazz -> clazz.annotations.let { annos -> annos.any { it.annotationClass == NigateBean::class } } }
                ?.also { scans.add(it) }
        }
    }
}