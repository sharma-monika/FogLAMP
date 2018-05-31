#include <gtest/gtest.h>
#include <config_category.h>
#include <string.h>
#include <string>
#include <rapidjson/document.h>

using namespace std;
using namespace rapidjson;

const char *categories = "{\"categories\": ["
	"{\"key\": \"cat1\", \"description\":\"First category\"},"
	"{\"key\": \"cat2\", \"description\":\"Second\"}]}";

const char *myCategory = "{\"description\": {"
		"\"value\": \"The FogLAMP administrative API\","
		"\"type\": \"string\","
		"\"default\": \"The FogLAMP administrative API\","
		"\"description\": \"The description of this FogLAMP service\"},"
	"\"name\": {"
		"\"value\": \"FogLAMP\","
		"\"type\": \"string\","
		"\"default\": \"FogLAMP\","
		"\"description\": \"The name of this FogLAMP service\"}}";

const char *json = "{ \"key\" : \"test\", \"description\" : \"Test description\", "
	"\"description\" : { "
		"\"description\" : \"The description of this FogLAMP service\", "
		"\"type\" : \"string\", "
		"\"value\" : \"The FogLAMP administrative API\", "
		"\"default\" : \"The FogLAMP administrative API\" }, "
	"\"name\" : { "
		"\"description\" : \"The name of this FogLAMP service\", "
		"\"type\" : \"string\", "
		"\"value\" : \"FogLAMP\", "
		"\"default\" : \"FogLAMP\" }}";

TEST(CategoriesTest, Count)
{
	ConfigCategories confCategories(categories);
	ASSERT_EQ(2, confCategories.length());
}

TEST(CategoriesTest, Index)
{
	ConfigCategories confCategories(categories);
	const ConfigCategoryDescription *item = confCategories[0];
	ASSERT_EQ(0, item->getName().compare("cat1"));
	ASSERT_EQ(0, item->getDescription().compare("First category"));
}

TEST(CategoryTest, Construct)
{
	ConfigCategory confCategory("test", myCategory);
	ASSERT_EQ(2, confCategory.getCount());
}

TEST(CategoryTest, ExistsTest)
{
	ConfigCategory confCategory("test", myCategory);
	ASSERT_EQ(true, confCategory.itemExists("name"));
	ASSERT_EQ(false, confCategory.itemExists("non-existance"));
}

TEST(CategoryTest, getValue)
{
	ConfigCategory confCategory("test", myCategory);
	ASSERT_EQ(0, confCategory.getValue("name").compare("FogLAMP"));
}

TEST(CategoryTest, getType)
{
	ConfigCategory confCategory("test", myCategory);
	ASSERT_EQ(0, confCategory.getType("name").compare("string"));
}

TEST(CategoryTest, getDefault)
{
	ConfigCategory confCategory("test", myCategory);
	ASSERT_EQ(0, confCategory.getDefault("name").compare("FogLAMP"));
}

TEST(CategoryTest, getDescription)
{
	ConfigCategory confCategory("test", myCategory);
	ASSERT_EQ(0, confCategory.getDescription("name").compare("The name of this FogLAMP service"));
}

TEST(CategoryTest, toJSON)
{
	ConfigCategory confCategory("test", myCategory);
	confCategory.setDescription("Test description");
	ASSERT_EQ(0, confCategory.toJSON().compare(json));
}
