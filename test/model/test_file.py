# @pytest.mark.parametrize(
#     "dirpath, filename, id",
#     [
#         ("/", "test", None),
#         ("/test", "basic", 42),
#         ("/test/basic", "small", 69),
#         ("/test/random", "test", (2**31) - 1),
#     ],
# )
# def test_file_init(mock_directory, dirpath, filename, id):
#     # Arrange
#     parent = mock_directory.from_path(dirpath)
#     # Act
#     file = File(name=filename, parent=parent, id=id)
#     # Assert
#     assert file.name == filename
#     assert file.parent == parent
#     assert file.id == id
#

